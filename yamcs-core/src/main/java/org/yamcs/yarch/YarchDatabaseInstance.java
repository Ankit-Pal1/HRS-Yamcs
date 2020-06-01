package org.yamcs.yarch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.ConfigurationException;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsServer;
import org.yamcs.archive.TagDb;
import org.yamcs.management.ManagementService;
import org.yamcs.utils.YObjectLoader;
import org.yamcs.utils.parser.ParseException;
import org.yamcs.yarch.rocksdb.RdbStorageEngine;
import org.yamcs.yarch.rocksdb.protobuf.Tablespace.BucketProperties;
import org.yamcs.yarch.streamsql.ExecutionContext;
import org.yamcs.yarch.streamsql.ResultListener;
import org.yamcs.yarch.streamsql.StreamSqlException;
import org.yamcs.yarch.streamsql.StreamSqlParser;
import org.yamcs.yarch.streamsql.StreamSqlStatement;
import org.yamcs.yarch.streamsql.TokenMgrError;
import org.yaml.snakeyaml.Yaml;

/**
 * Handles tables and streams for one Yamcs Instance
 * 
 * 
 * Synchronisation policy: to avoid problems with stream disappearing when clients connect to them, all the
 * creation/closing/subscription to streams/tables shall be done while acquiring a lock on the YarchDatabase object.
 * This is done in the StreamSqlStatement.java
 * 
 * Delivery of tuples does not require locking, this means subscription can change while delivering (for that a
 * concurrent list is used in Stream.java)
 * 
 * @author nm
 *
 */
public class YarchDatabaseInstance {

    private static final Logger log = LoggerFactory.getLogger(YarchDatabaseInstance.class.getName());

    Map<String, TableDefinition> tables = new HashMap<>();
    transient Map<String, Stream> streams = new HashMap<>();

    String tablespaceName;

    BucketDatabase bucketDatabase;
    FileSystemBucketDatabase fileSystemBucketDatabase;

    final ManagementService managementService;

    // yamcs instance name (used to be called dbname)
    private String instanceName;

    YarchDatabaseInstance(String instanceName) throws YarchException {
        this.instanceName = instanceName;
        managementService = ManagementService.getInstance();

        String instConfName = "yamcs." + instanceName;
        YConfiguration yconf;
        if (YConfiguration.isDefined(instConfName)) {
            yconf = YConfiguration.getConfiguration(instConfName);
            if (yconf.containsKey("tablespace")) {
                tablespaceName = yconf.getString("tablespace");
            } else {
                tablespaceName = instanceName;
            }

            if (yconf.containsKey("bucketDatabase")) {
                YConfiguration dbConfig = yconf.getConfig("bucketDatabase");
                loadBucketDatabase(dbConfig);
            }
        } else {
            yconf = YConfiguration.getConfiguration("yamcs");
            tablespaceName = instanceName;

            if (yconf.containsKey("bucketDatabase")) {
                YConfiguration dbConfig = yconf.getConfig("bucketDatabase");
                loadBucketDatabase(dbConfig);
            }
        }
        loadTables();

        if (bucketDatabase == null) {
            bucketDatabase = YarchDatabase.getDefaultStorageEngine().getBucketDatabase(this);
        }
        try {
            fileSystemBucketDatabase = new FileSystemBucketDatabase(instanceName);
        } catch (IOException e) {
            throw new YarchException("Failed to load file-system based bucket database", e);
        }

        if (yconf.containsKey("buckets")) {
            List<YConfiguration> bucketsConfig = yconf.getConfigList("buckets");
            loadBuckets(bucketsConfig);
        }
    }

    private BucketDatabase loadBucketDatabase(YConfiguration config) {
        String clazz = config.getString("class");
        Object args = config.get("args");
        try {
            if (args == null) {
                bucketDatabase = YObjectLoader.loadObject(clazz, instanceName);
            } else {
                bucketDatabase = YObjectLoader.loadObject(clazz, instanceName, args);
            }
        } catch (IOException e) {
            throw new ConfigurationException("Failed to load bucket database: " + e.getMessage(), e);
        }
        return bucketDatabase;
    }

    /**
     * Loads pre-defined buckets. The buckets will be created if they do not exist yet. By using the <code>path</code>
     * argument, it is possible to map a bucket to a random file system location instead of the default bucket storage
     * engine of this Yarch instance.
     */
    private void loadBuckets(List<YConfiguration> configs) {
        try {
            for (YConfiguration config : configs) {
                String name = config.getString("name");
                if (config.containsKey("path")) {
                    Path path = Paths.get(config.getString("path"));
                    addFileSystemBucket(name, path);
                } else {
                    Bucket bucket = getBucket(name);
                    if (bucket == null) {
                        log.info("Creating bucket {}", name);
                        createBucket(name);
                    }
                }
            }
        } catch (IOException e) {
            throw new ConfigurationException("Failed to load buckets: " + e.getMessage(), e);
        }
    }

    /**
     * Tablespaces are used by {@link RdbStorageEngine} to store data. Returns the default tablespace name that is used
     * by all tables and also the parameter archive of this yamcs instance
     * 
     */
    public String getTablespaceName() {
        return tablespaceName;
    }

    /**
     * 
     * @return the instance name
     */
    public String getName() {
        return instanceName;
    }

    public String getYamcsInstance() {
        return instanceName;
    }

    /**
     * loads all the .def files from the disk. The ascii def file is structed as follows col1 type1, col2 type2, col3
     * type3 <- definition of the columns col1, col2 <- definition of the primary key
     * 
     * @throws YarchException
     */
    void loadTables() throws YarchException {
        File dir = new File(getRoot());
        if (dir.exists()) {
            File[] dirFiles = dir.listFiles();
            if (dirFiles == null) {
                return; // no tables found
            }
            for (File f : dirFiles) {
                String fn = f.getName();
                if (fn.endsWith(".def")) {
                    try {
                        TableDefinition tblDef = deserializeTableDefinition(f);
                        StorageEngine storageEngine = getStorageEngine(tblDef);
                        if (storageEngine == null) {
                            throw new YarchException("Do not have a storage engine '" + tblDef.getStorageEngineName()
                                    + "'. Check storageEngines key in yamcs.yaml");
                        }

                        getStorageEngine(tblDef).loadTable(this, tblDef);
                        managementService.registerTable(instanceName, tblDef);
                        tables.put(tblDef.getName(), tblDef);
                        log.debug("Loaded table definition {} from {}", tblDef.getName(), f);
                    } catch (IOException e) {
                        log.warn("Got exception when reading the table definition from {}: ", f, e);
                        throw new YarchException("Got exception when reading the table definition from " + f + ": ", e);
                    } catch (ClassNotFoundException e) {
                        log.warn("Got exception when reading the table definition from {}: ", f, e);
                        throw new YarchException("Got exception when reading the table definition from " + f + ": ", e);
                    }
                }
            }
        } else {
            log.info("Creating directory for db {}: {}", instanceName, dir.getAbsolutePath());
            if (!dir.mkdirs()) {
                YamcsServer.getServer().getCrashHandler(instanceName).handleCrash("Archive",
                        "Cannot create directory: " + dir);
                log.error("Cannot create directory: {}", dir);
            }
        }
    }

    TableDefinition deserializeTableDefinition(File f) throws IOException, ClassNotFoundException {
        if (f.length() == 0) {
            throw new IOException("Cannot load table definition from empty file " + f);
        }
        String fn = f.getName();
        String tblName = fn.substring(0, fn.length() - 4);
        Yaml yaml = new Yaml(new TableDefinitionConstructor());
        FileInputStream fis = new FileInputStream(f);
        Object o = yaml.load(fis);
        if (!(o instanceof TableDefinition)) {
            fis.close();
            throw new IOException("Cannot load table definition from " + f + ": object is " + o.getClass().getName()
                    + "; should be " + TableDefinition.class.getName());
        }
        TableDefinition tblDef = (TableDefinition) o;
        fis.close();

        tblDef.setName(tblName);
        tblDef.setDb(this);
        if (!tblDef.hasCustomDataDir()) {
            tblDef.setDataDir(getRoot());
        }

        if (tblDef.getFormatVersion() != TableDefinition.CURRENT_FORMAT_VERSION) {
            // temporary upgrade to version 2 from version 1 - should be removed in a future version
            if (tblDef.getFormatVersion() == 1) {
                log.info("Converting {} from format version 1 to format version 2", tblDef.getName());
                if ("pp".equals(tblDef.getName())) {
                    changeParaValueType(tblDef);
                }
                tblDef.setFormatVersion(2);
                serializeTableDefinition(tblDef);
                return deserializeTableDefinition(f);
            }
        }

        log.debug("Loaded table definition {} from {}", tblName, fn);
        return tblDef;
    }

    static void changeParaValueType(TableDefinition tblDef) {
        TupleDefinition valueDef = tblDef.getValueDefinition();
        List<ColumnDefinition> l = valueDef.getColumnDefinitions();
        for (int i = 0; i < l.size(); i++) {
            ColumnDefinition cd = l.get(i);
            if ("PROTOBUF(org.yamcs.protobuf.Pvalue$ParameterValue)".equals(cd.getType().name())) {
                ColumnDefinition cd1 = new ColumnDefinition(cd.getName(), DataType.PARAMETER_VALUE);
                l.set(i, cd1);
            }
        }
    }

    /**
     * serializes to disk to the rootDir/name.def
     * 
     * @param algorithmDef
     */
    void serializeTableDefinition(TableDefinition td) {
        String fn = getRoot() + "/" + td.getName() + ".def";
        try (FileOutputStream fos = new FileOutputStream(fn)) {
            Yaml yaml = new Yaml(new TableDefinitionRepresenter());

            Writer w = new BufferedWriter(new OutputStreamWriter(fos));
            yaml.dump(td, w);
            w.flush();
            fos.getFD().sync();
            w.close();
        } catch (IOException e) {
            YamcsServer.getServer().getCrashHandler(instanceName).handleCrash("Archive",
                    "Cannot write table definition to " + fn + " :" + e);
            log.error("Got exception when writing table definition to {} ", fn, e);
        }
    }

    /**
     * add a table to the dictionary throws exception if a table or a stream with the same name already exist
     * 
     * @param def
     *            - table definition
     * @throws YarchException
     *             - thrown in case a table or a stream with the same name already exists or if there was an error in
     *             creating the table
     * 
     */
    public void createTable(TableDefinition def) throws YarchException {
        if (tables.containsKey(def.getName())) {
            throw new YarchException("A table named '" + def.getName() + "' already exists");
        }
        if (streams.containsKey(def.getName())) {
            throw new YarchException("A stream named '" + def.getName() + "' already exists");
        }

        if (!def.hasCustomDataDir()) {
            def.setDataDir(getRoot());
        }
        StorageEngine se = YarchDatabase.getStorageEngine(def.getStorageEngineName());
        if (se == null) {
            throw new YarchException("Invalid storage engine '" + def.getStorageEngineName()
                    + "' specified. Valid names are: " + YarchDatabase.getStorageEngineNames());
        }
        se.createTable(this, def);

        tables.put(def.getName(), def);
        def.setDb(this);
        serializeTableDefinition(def);
        if (managementService != null) {
            managementService.registerTable(instanceName, def);
        }
    }

    /**
     * Adds a stream to the dictionary making it "official"
     * 
     * @param stream
     * @throws YarchException
     */
    public void addStream(Stream stream) throws YarchException {
        if (tables.containsKey(stream.getName())) {
            throw new YarchException("A table named '" + stream.getName() + "' already exists");
        }
        if (streams.containsKey(stream.getName())) {
            throw new YarchException("A stream named '" + stream.getName() + "' already exists");
        }
        streams.put(stream.getName(), stream);
        if (managementService != null) {
            managementService.registerStream(instanceName, stream);
        }
    }

    public TableDefinition getTable(String name) {
        return tables.get(name);
    }

    public boolean streamOrTableExists(String name) {
        if (streams.containsKey(name)) {
            return true;
        }
        if (tables.containsKey(name)) {
            return true;
        }
        return false;
    }

    public Stream getStream(String name) {
        return streams.get(name);
    }

    public void dropTable(String tblName) throws YarchException {
        log.info("Dropping table {}", tblName);
        TableDefinition tbl = tables.remove(tblName);
        if (tbl == null) {
            throw new YarchException("There is no table named '" + tblName + "'");
        }
        if (managementService != null) {
            managementService.unregisterTable(instanceName, tblName);
        }
        getStorageEngine(tbl).dropTable(this, tbl);
        File f = new File(getRoot() + "/" + tblName + ".def");
        if (!f.delete()) {
            throw new YarchException("Cannot remove " + f);
        }
    }

    public synchronized void removeStream(String name) {
        Stream s = streams.remove(name);
        if ((s != null) && (managementService != null)) {
            managementService.unregisterStream(instanceName, name);
        }
    }

    public StorageEngine getStorageEngine(TableDefinition tbldef) {
        return YarchDatabase.getStorageEngine(tbldef.getStorageEngineName());
    }

    public Collection<Stream> getStreams() {
        return streams.values();
    }

    public Collection<TableDefinition> getTableDefinitions() {
        return tables.values();
    }

    /**
     * Returns the root directory for this database instance. It is usually home/instance_name.
     */
    public String getRoot() {
        return YarchDatabase.getHome() + "/" + instanceName;
    }

    public StreamSqlStatement createStatement(String query, Object... args) throws StreamSqlException, ParseException {
        StreamSqlParser parser = new StreamSqlParser(new java.io.StringReader(query));
        parser.setArgs(args);
        try {
            return parser.OneStatement();
        } catch (TokenMgrError e) {
            throw new ParseException(e.getMessage());
        }
    }

    public void execute(StreamSqlStatement stmt, ResultListener resultListener)
            throws StreamSqlException, ParseException {
        ExecutionContext context = new ExecutionContext(instanceName);
        stmt.execute(context, resultListener);
    }

    public void execute(String query, Object... args) throws StreamSqlException, ParseException {
        StreamSqlStatement stmt = createStatement(query, args);
        execute(stmt, new ResultListener() { // Discards everything

            @Override
            public void next(Tuple tuple) {
            }

            @Override
            public void completeExceptionally(Throwable t) {
            }

            @Override
            public void complete() {
            }
        });
    }

    public void close() {
        // make a copy such that we don't get ConcurrentModificationException when stream.close will cause it to be
        // removed from the map
        List<Stream> l = new ArrayList<>(streams.values());
        for (Stream s : l) {
            s.close();
        }
    }

    public TagDb getTagDb() throws YarchException {
        return YarchDatabase.getDefaultStorageEngine().getTagDb(this);
    }

    public ProtobufDatabase getProtobufDatabase() throws YarchException {
        return YarchDatabase.getDefaultStorageEngine().getProtobufDatabase(this);
    }

    public TimePartitionSchema getDefaultPartitioningSchema() {
        return TimePartitionSchema.getInstance("YYYY");
    }

    public Bucket getBucket(String bucketName) throws IOException {
        Bucket bucket = fileSystemBucketDatabase.getBucket(bucketName);
        if (bucket != null) {
            return bucket;
        }

        return bucketDatabase.getBucket(bucketName);
    }

    public Bucket createBucket(String bucketName) throws IOException {
        return bucketDatabase.createBucket(bucketName);
    }

    /**
     * Creates a bucket that maps to the file system. This bucket will be located in a default folder.
     * 
     * @param bucketName
     *            the name of the bucket
     * @return the created bucket
     * @throws IOException
     *             on I/O issues
     * @deprecated Use {@link #addFileSystemBucket(String, Path)}
     */
    @Deprecated
    public FileSystemBucket createFileSystemBucket(String bucketName) throws IOException {
        FileSystemBucket bucket = fileSystemBucketDatabase.getBucket(bucketName);
        if (bucket == null) {
            bucket = fileSystemBucketDatabase.createBucket(bucketName);
        }
        return bucket;
    }

    /**
     * Creates a bucket that maps to the file system.
     * 
     * @param bucketName
     *            the name of the bucket
     * @param location
     *            the path to the bucket contents
     * @return the created bucket
     * @throws IOException
     *             on I/O issues
     * @deprecated Use {@link #addFileSystemBucket(String, Path)}
     */
    @Deprecated
    public FileSystemBucket createFileSystemBucket(String bucketName, Path location) throws IOException {
        return addFileSystemBucket(bucketName, location);
    }

    /**
     * Adds a bucket that maps to the file system. This is a transient operation that has to be done on each server
     * restart.
     * 
     * @param bucketName
     *            the name of the bucket
     * @param location
     *            the path to the bucket contents
     * @return the created bucket
     * @throws IOException
     *             on I/O issues
     */
    public FileSystemBucket addFileSystemBucket(String bucketName, Path location) throws IOException {
        return fileSystemBucketDatabase.registerBucket(bucketName, location);
    }

    public List<BucketProperties> listBuckets() throws IOException {
        List<BucketProperties> buckets = new ArrayList<>(fileSystemBucketDatabase.listBuckets());
        List<String> names = buckets.stream().map(b -> b.getName()).collect(Collectors.toList());

        for (BucketProperties bucket : bucketDatabase.listBuckets()) {
            if (!names.contains(bucket.getName())) {
                buckets.add(bucket);
            }
        }

        return buckets;
    }

    public void deleteBucket(String bucketName) throws IOException {
        if (fileSystemBucketDatabase.getBucket(bucketName) != null) {
            fileSystemBucketDatabase.deleteBucket(bucketName);
        } else {
            bucketDatabase.deleteBucket(bucketName);
        }
    }
}
