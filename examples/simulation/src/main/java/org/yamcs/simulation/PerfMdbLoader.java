package org.yamcs.simulation;

import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.yamcs.ConfigurationException;
import org.yamcs.YConfiguration;
import org.yamcs.xtce.Comparison;
import org.yamcs.xtce.DatabaseLoadException;
import org.yamcs.xtce.IntegerDataEncoding;
import org.yamcs.xtce.IntegerParameterType;
import org.yamcs.xtce.OperatorType;
import org.yamcs.xtce.Parameter;
import org.yamcs.xtce.ParameterEntry;
import org.yamcs.xtce.ParameterInstanceRef;
import org.yamcs.xtce.SequenceContainer;
import org.yamcs.xtce.SequenceEntry;
import org.yamcs.xtce.SequenceEntry.ReferenceLocationType;
import org.yamcs.xtce.util.NameReference.Type;
import org.yamcs.xtce.util.UnresolvedNameReference;
import org.yamcs.xtce.SpaceSystem;
import org.yamcs.xtce.SpaceSystemLoader;

/**
 * Generates a MDB used for performance testing.
 * 
 * It generates all unsigned integer parameters with the size in bits specified
 * 
 * @author nm
 *
 */
public class PerfMdbLoader implements SpaceSystemLoader {
    int numPackets;
    int packetSize;
    int numParam;
    int paramSizeInBits;

    static String PACKET_ID_PARA_NAME = "packet-id";

    public PerfMdbLoader(YConfiguration config) {
        numPackets = config.getInt("numPackets");
        packetSize = config.getInt("packetSize");
        paramSizeInBits = config.getInt("paramSizeInBits", 32);
        numParam = packetSize * 8 / paramSizeInBits;
    }

    @Override
    public boolean needsUpdate(RandomAccessFile consistencyDateFile) throws IOException, ConfigurationException {
        return true;
    }

    @Override
    public String getConfigName() {
        return "perf-data";
    }

    @Override
    public void writeConsistencyDate(FileWriter consistencyDateFile) {
        return;
    }

    @Override
    public SpaceSystem load() throws ConfigurationException, DatabaseLoadException {
        SpaceSystem ss = new SpaceSystem("perf-data");
        
        IntegerParameterType ptype = new IntegerParameterType("uint"+paramSizeInBits);
        ptype.setSizeInBits(paramSizeInBits);
        IntegerDataEncoding ide = new IntegerDataEncoding(paramSizeInBits);
        ptype.setEncoding(ide);
        ss.addParameterType(ptype);
        for (int j = 0; j < numPackets; j++) {
            int pktId = PerfPacketGenerator.PERF_TEST_PACKET_ID + j;
            SequenceContainer sc = new SequenceContainer("pkt_" + pktId);
            UnresolvedNameReference unr = new UnresolvedNameReference("/YSS/ccsds-default", Type.SEQUENCE_CONTAINER);

            unr.addResolvedAction(nd -> {
                addCcsdsInheritance((SequenceContainer) nd, sc, pktId);
                return true;
            });
            ss.addUnresolvedReference(unr);
            for (int i = 0; i < numParam; i++) {
                Parameter p = new Parameter("p_" + pktId + "_"+ptype.getName()+"_" + i);
                p.setParameterType(ptype);
                ParameterEntry pe = new ParameterEntry(128 + paramSizeInBits * i, ReferenceLocationType.containerStart, p);
                sc.addEntry(pe);
                ss.addParameter(p);
            }
            ss.addSequenceContainer(sc);
        }
        return ss;
    }

    private void addCcsdsInheritance(SequenceContainer ccsds, SequenceContainer sc, int id) {
        for (SequenceEntry se : ccsds.getEntryList()) {
            if (se instanceof ParameterEntry) {
                ParameterEntry pe = (ParameterEntry) se;
                if (PACKET_ID_PARA_NAME.equals(pe.getParameter().getName())) {
                    Parameter packetIdParam = pe.getParameter();
                    Comparison c = new Comparison(new ParameterInstanceRef(packetIdParam), id, OperatorType.EQUALITY);
                    sc.setBaseContainer(ccsds);
                    sc.setRestrictionCriteria(c);
                    return;
                }
            }
        }
        throw new ConfigurationException(
                "Cannot find a parameter '" + PACKET_ID_PARA_NAME + "' in the container " + ccsds.getName());
    }

}
