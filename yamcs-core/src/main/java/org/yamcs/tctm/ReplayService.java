package org.yamcs.tctm;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.yamcs.AbstractProcessorService;
import org.yamcs.ConfigurationException;
import org.yamcs.InvalidIdentification;
import org.yamcs.NoPermissionException;
import org.yamcs.Processor;
import org.yamcs.ProcessorException;
import org.yamcs.TmPacket;
import org.yamcs.TmProcessor;
import org.yamcs.YConfiguration;
import org.yamcs.YamcsException;
import org.yamcs.YamcsServer;
import org.yamcs.archive.ReplayListener;
import org.yamcs.archive.ReplayOptions;
import org.yamcs.archive.ReplayServer;
import org.yamcs.archive.XtceTmReplayHandler.ReplayPacket;
import org.yamcs.archive.YarchReplay;
import org.yamcs.cmdhistory.CommandHistoryProvider;
import org.yamcs.cmdhistory.CommandHistoryRequestManager;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.parameter.ParameterListener;
import org.yamcs.parameter.ParameterProvider;
import org.yamcs.parameter.ParameterRequestManager;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.parameter.ParameterWithIdRequestHelper;
import org.yamcs.protobuf.Commanding.CommandHistoryEntry;
import org.yamcs.protobuf.Yamcs.CommandHistoryReplayRequest;
import org.yamcs.protobuf.Yamcs.EndAction;
import org.yamcs.protobuf.Yamcs.EventReplayRequest;
import org.yamcs.protobuf.Yamcs.NamedObjectId;
import org.yamcs.protobuf.Yamcs.NamedObjectList;
import org.yamcs.protobuf.Yamcs.PacketReplayRequest;
import org.yamcs.protobuf.Yamcs.PpReplayRequest;
import org.yamcs.protobuf.Yamcs.ProtoDataType;
import org.yamcs.protobuf.Yamcs.ReplayRequest;
import org.yamcs.protobuf.Yamcs.ReplaySpeed;
import org.yamcs.protobuf.Yamcs.ReplaySpeed.ReplaySpeedType;
import org.yamcs.protobuf.Yamcs.ReplayStatus;
import org.yamcs.protobuf.Yamcs.ReplayStatus.ReplayState;
import org.yamcs.security.SecurityStore;
import org.yamcs.xtce.Parameter;
import org.yamcs.xtce.SequenceContainer;
import org.yamcs.xtce.XtceDb;
import org.yamcs.xtceproc.ParameterTypeProcessor;
import org.yamcs.xtceproc.Subscription;
import org.yamcs.xtceproc.XtceDbFactory;
import org.yamcs.xtceproc.XtceTmProcessor;
import org.yamcs.yarch.protobuf.Db.Event;

import com.google.protobuf.util.JsonFormat;

/**
 * Provides telemetry packets and processed parameters from the yamcs archive.
 * 
 * @author nm
 * 
 */
public class ReplayService extends AbstractProcessorService
        implements ReplayListener, ArchiveTmPacketProvider, ParameterProvider, CommandHistoryProvider {
    static final long TIMEOUT = 10000;

    EndAction endAction;

    ReplayOptions originalReplayRequest;
    private HashSet<Parameter> subscribedParameters = new HashSet<>();
    private ParameterRequestManager parameterRequestManager;
    TmProcessor tmProcessor;
    XtceDb xtceDb;
    volatile long replayTime;

    YarchReplay yarchReplay;
    // the originalReplayRequest contains possibly only parameters.
    // the modified one sent to the ReplayServer contains the raw data required for extracting/processing those
    // parameters
    ReplayOptions rawDataRequest;
    CommandHistoryRequestManager commandHistoryRequestManager;

    private SecurityStore securityStore;

    // this can be set in the config (in processor.yaml) to exclude certain paramter groups from replay
    List<String> excludeParameterGroups = null;

    @Override
    public void init(Processor proc, YConfiguration args, Object spec) {
        super.init(proc, args, spec);
        if(spec == null) {
            throw new IllegalArgumentException("Please provide the spec");
        }
        xtceDb = XtceDbFactory.getInstance(getYamcsInstance());
        securityStore = YamcsServer.getServer().getSecurityStore();
        if (args.containsKey("excludeParameterGroups")) {
            excludeParameterGroups = args.getList("excludeParameterGroups");
        }
        this.tmProcessor = proc.getTmProcessor();
        proc.setCommandHistoryProvider(this);
        parameterRequestManager = proc.getParameterRequestManager();
        proc.setPacketProvider(this);
        parameterRequestManager.addParameterProvider(this);

        if (spec instanceof ReplayOptions) {
            this.originalReplayRequest = (ReplayOptions) spec;
        } else if (spec instanceof String) {
            ReplayRequest.Builder rrb = ReplayRequest.newBuilder();
            try {
                JsonFormat.parser().merge((String) spec, rrb);
            } catch (IOException e) {
                throw new ConfigurationException("Cannot parse config into a replay request: " + e.getMessage(), e);
            }
            if (!rrb.hasSpeed()) {
                rrb.setSpeed(ReplaySpeed.newBuilder().setType(ReplaySpeedType.REALTIME).setParam(1));
            }
            this.originalReplayRequest = new ReplayOptions(rrb.build());
        } else {
            throw new IllegalArgumentException("Unknown spec of type " + spec.getClass());
        }
    }

    @Override
    public boolean isArchiveReplay() {
        return true;
    }

    @Override
    public void newData(ProtoDataType type, Object data) {

        switch (type) {
        case TM_PACKET:
            ReplayPacket rp = (ReplayPacket) data;
            replayTime = rp.getGenerationTime();
            String qn = rp.getQualifiedName();
            SequenceContainer container = xtceDb.getSequenceContainer(qn);
            if (container == null) {
                log.warn("Unknown sequence container '" + qn + "' found when replaying", qn);
            } else {
                SequenceContainer parent;
                while ((parent = container.getBaseContainer()) != null) {
                    container = parent;
                }

                tmProcessor.processPacket(new TmPacket(rp.getReceptionTime(), rp.getGenerationTime(),
                        rp.getSequenceNumber(), rp.getPacket()), container);
            }
            break;
        case PP:
            List<ParameterValue> pvals = (List<ParameterValue>) data;
            if (!pvals.isEmpty()) {
                replayTime = pvals.get(0).getGenerationTime();
                parameterRequestManager.update(calibrate(pvals));
            }
            break;
        case CMD_HISTORY:
            CommandHistoryEntry che = (CommandHistoryEntry) data;
            replayTime = che.getCommandId().getGenerationTime();
            commandHistoryRequestManager.addCommand(PreparedCommand.fromCommandHistoryEntry(che));
            break;
        case EVENT:
            Event evt = (Event) data;
            replayTime = evt.getGenerationTime();
            break;
        default:
            log.error("Unexpected data type {} received", type);
        }
    }

    private List<ParameterValue> calibrate(List<ParameterValue> pvlist) {
        ParameterTypeProcessor ptypeProcessor = processor.getProcessorData().getParameterTypeProcessor();

        for (ParameterValue pv : pvlist) {
            if (pv.getEngValue() == null && pv.getRawValue() != null) {
                ptypeProcessor.calibrate(pv);
            }
        }
        return pvlist;
    }

    @Override
    public void stateChanged(ReplayStatus rs) {
        if (rs.getState() == ReplayState.CLOSED) {
            log.debug("End signal received");
            notifyStopped();
            tmProcessor.finished();
        } else {
            processor.notifyStateChange();
        }
    }

    @Override
    public void doStop() {
        if (yarchReplay != null) {
            yarchReplay.quit();
        }
        notifyStopped();
    }

    // finds out all raw data (TM and PP) required to provide the needed parameters.
    // in order to do this, subscribe to all parameters from the list, then check in the tmProcessor subscription which
    // containers are needed and in the subscribedParameters which PPs may be required
    private void createRawSubscription() throws YamcsException {

        boolean replayAll = originalReplayRequest.isReplayAll();

        if (replayAll) {
            rawDataRequest = new ReplayOptions(originalReplayRequest);
            rawDataRequest.setPacketRequest(PacketReplayRequest.newBuilder().build());
            rawDataRequest.setEventRequest(EventReplayRequest.newBuilder().build());
            rawDataRequest.setPpRequest(PpReplayRequest.newBuilder().build());
            rawDataRequest.setCommandHistoryRequest(CommandHistoryReplayRequest.newBuilder().build());
        } else {
            rawDataRequest = new ReplayOptions(originalReplayRequest);
            rawDataRequest.clearParameterRequest();
        }

        if (!replayAll) {
            addPacketsRequiredForParams();
        }

        // now check for PPs
        Set<String> pprecordings = new HashSet<>();

        for (Parameter p : subscribedParameters) {
            pprecordings.add(p.getRecordingGroup());
        }
        if (pprecordings.isEmpty() && excludeParameterGroups == null) {
            log.debug("No additional pp group added or removed to/from the subscription");
        } else {
            PpReplayRequest ppreq = originalReplayRequest.getPpRequest();
            PpReplayRequest.Builder pprr = ppreq.toBuilder();
            pprr.addAllGroupNameFilter(pprecordings);
            if (excludeParameterGroups != null) {
                pprr.addAllGroupNameExclude(excludeParameterGroups);
            }
            rawDataRequest.setPpRequest(pprr.build());

        }
        if (!rawDataRequest.hasPacketRequest() && !rawDataRequest.hasPpRequest()) {
            if (originalReplayRequest.hasParameterRequest()) {
                throw new YamcsException("Cannot find a replay source for any parmeters from request: "
                        + originalReplayRequest.getParameterRequest().toString());
            } else {
                throw new YamcsException("Refusing to create an empty replay request");
            }
        }
    }

    private void addPacketsRequiredForParams() throws YamcsException {
        List<NamedObjectId> plist = originalReplayRequest.getParameterRequest().getNameFilterList();
        if (plist.isEmpty()) {
            return;
        }
        ParameterWithIdRequestHelper pidrm = new ParameterWithIdRequestHelper(parameterRequestManager,
                (subscriptionId, params) -> {
                    // ignore data, we create this subscription just to get the list of
                    // dependent containers and PPs
                });
        int subscriptionId;
        try {
            subscriptionId = pidrm.addRequest(plist, securityStore.getSystemUser());
        } catch (InvalidIdentification e) {
            NamedObjectList nol = NamedObjectList.newBuilder().addAllList(e.getInvalidParameters()).build();
            throw new YamcsException("InvalidIdentification", "Invalid identification", nol);
        } catch (NoPermissionException e) {
            throw new IllegalStateException("Unexpected No permission");
        }

        XtceTmProcessor tmproc = processor.getTmProcessor();
        Subscription subscription = tmproc.getSubscription();
        Collection<SequenceContainer> containers = subscription.getContainers();

        if ((containers == null) || (containers.isEmpty())) {
            log.debug("No container required for the parameter subscription");
        } else {
            PacketReplayRequest.Builder rawPacketRequest = originalReplayRequest.getPacketRequest().toBuilder();

            for (SequenceContainer sc : containers) {
                rawPacketRequest.addNameFilter(NamedObjectId.newBuilder().setName(sc.getQualifiedName()).build());
            }
            log.debug("after TM subscription, the request contains the following packets: "
                    + rawPacketRequest.getNameFilterList());
            rawDataRequest.setPacketRequest(rawPacketRequest.build());
        }
        pidrm.removeRequest(subscriptionId);
    }

    private void createReplay() throws ProcessorException {
        List<ReplayServer> services = YamcsServer.getServer().getServices(getYamcsInstance(), ReplayServer.class);
        if (services.isEmpty()) {
            throw new ProcessorException("ReplayServer not configured for this instance");
        }
        try {
            ReplayServer replayServer = services.get(0);
            yarchReplay = replayServer.createReplay(rawDataRequest, this);
        } catch (YamcsException e) {
            log.error("Exception creating the replay", e);
            throw new ProcessorException("Exception creating the replay: " + e.getMessage(), e);
        }
    }

    @Override
    public void doStart() {
        try {
            createRawSubscription();
            createReplay();
        } catch (YamcsException e) {
            notifyFailed(e);
            return;
        }

        yarchReplay.start();
        notifyStarted();
    }

    @Override
    public void pause() {
        yarchReplay.pause();
    }

    @Override
    public void resume() {
        yarchReplay.start();
    }

    @Override
    public void seek(long time) {
        try {
            yarchReplay.seek(time);
        } catch (YamcsException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setParameterListener(ParameterListener parameterRequestManager) {
        this.parameterRequestManager = (ParameterRequestManager) parameterRequestManager;
    }

    @Override
    public void startProviding(Parameter paramDef) {
        synchronized (subscribedParameters) {
            subscribedParameters.add(paramDef);
        }
    }

    @Override
    public void startProvidingAll() {
        // TODO
    }

    @Override
    public void stopProviding(Parameter paramDef) {
        synchronized (subscribedParameters) {
            subscribedParameters.remove(paramDef);
        }
    }

    @Override
    public boolean canProvide(NamedObjectId id) {
        boolean result = false;
        Parameter p = xtceDb.getParameter(id);
        if (p != null) {
            result = canProvide(p);
        } else { // check if it's system parameter
            if (XtceDb.isSystemParameter(id)) {
                result = true;
            }
        }
        return result;
    }

    @Override
    public boolean canProvide(Parameter p) {
        boolean result;
        if (xtceDb.getParameterEntries(p) != null) {
            result = false;
        } else {
            result = true;
        }
        return result;
    }

    @Override
    public Parameter getParameter(NamedObjectId id) throws InvalidIdentification {
        Parameter p = xtceDb.getParameter(id);
        if (p == null) {
            throw new InvalidIdentification();
        } else {
            return p;
        }
    }

    @Override
    public ReplaySpeed getSpeed() {
        return originalReplayRequest.getSpeed();
    }

    @Override
    public ReplayRequest getReplayRequest() {
        return originalReplayRequest.toProtobuf();
    }

    @Override
    public ReplayState getReplayState() {
        if (state() == State.NEW) {
            return ReplayState.INITIALIZATION;
        } else if (state() == State.FAILED) {
            return ReplayState.ERROR;
        } else {
            return yarchReplay.getState();
        }
    }

    @Override
    public long getReplayTime() {
        return replayTime;
    }

    @Override
    public void changeSpeed(ReplaySpeed speed) {
        yarchReplay.changeSpeed(speed);
        // need to change the replay request to get the proper value when getReplayRequest() is called
        originalReplayRequest.setSpeed(speed);
    }

    @Override
    public void setCommandHistoryRequestManager(CommandHistoryRequestManager chrm) {
        this.commandHistoryRequestManager = chrm;
    }

}
