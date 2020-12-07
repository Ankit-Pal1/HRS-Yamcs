package org.yamcs.cfdp.pdu;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yamcs.StandardTupleDefinitions;
import org.yamcs.cfdp.OngoingCfdpTransfer;
import org.yamcs.cfdp.CfdpTransactionId;
import org.yamcs.yarch.DataType;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.TupleDefinition;

public abstract class CfdpPacket {

    protected ByteBuffer buffer;
    protected CfdpHeader header;

    private static Logger log = LoggerFactory.getLogger("Packet");
    public static final TupleDefinition CFDP = new TupleDefinition();
    // outgoing CFDP packets
    static {
        CFDP.addColumn(StandardTupleDefinitions.GENTIME_COLUMN, DataType.TIMESTAMP);
        CFDP.addColumn("entityId", DataType.INT);
        CFDP.addColumn(StandardTupleDefinitions.SEQNUM_COLUMN, DataType.INT);
        CFDP.addColumn("pdu", DataType.BINARY);
    }

    protected CfdpPacket() {
        this.header = null;
    }

    protected CfdpPacket(CfdpHeader header) {
        this(null, header);
    }

    protected CfdpPacket(ByteBuffer buffer, CfdpHeader header) {
        this.header = header;
        this.buffer = buffer;
    }

    public CfdpHeader getHeader() {
        return this.header;
    }

    public abstract int getDataFieldLength();

    public static CfdpPacket getCFDPPacket(ByteBuffer buffer) {
        CfdpHeader header = new CfdpHeader(buffer);
        int limit = header.getLength() + CfdpHeader.getDataLength(buffer);
        if (limit > buffer.limit()) {
            throw new IllegalArgumentException("buffer too short, from header expected " + limit + " bytes, but only "
                    + buffer.limit() + " bytes available");
        }
        buffer.limit(limit);
        CfdpPacket toReturn = null;
        if (header.isFileDirective()) {
            switch (FileDirectiveCode.readFileDirectiveCode(buffer)) {
            case EOF:
                toReturn = new EofPacket(buffer, header);
                break;
            case FINISHED:
                toReturn = new FinishedPacket(buffer, header);
                break;
            case ACK:
                toReturn = new AckPacket(buffer, header);
                break;
            case METADATA:
                toReturn = new MetadataPacket(buffer, header);
                break;
            case NAK:
                toReturn = new NakPacket(buffer, header);
                break;
            case PROMPT:
                toReturn = new PromptPacket(buffer, header);
                break;
            case KEEP_ALIVE:
                toReturn = new KeepAlivePacket(buffer, header);
                break;
            default:
                break;
            }
        } else {
            toReturn = new FileDataPacket(buffer, header);
        }
        if (toReturn != null && header.withCrc()) {
            if (!toReturn.crcValid()) {
                log.error("invalid crc");
            }
        }
        return toReturn;
    }

    public byte[] toByteArray() {
        int dataLength = getDataFieldLength();
        ByteBuffer buffer = ByteBuffer.allocate(header.getLength() + dataLength);
        getHeader().writeToBuffer(buffer, dataLength);
        writeCFDPPacket(buffer);
        if (getHeader().withCrc()) {
            calculateAndAddCrc(buffer);
        }

        return buffer.array();
    }

    public void writeToBuffer(ByteBuffer buffer) {
        int dataLength = getDataFieldLength();
        header.writeToBuffer(buffer, dataLength);
        writeCFDPPacket(buffer);
        if (header.withCrc()) {
            calculateAndAddCrc(buffer);
        }
    }

    public Tuple toTuple(OngoingCfdpTransfer trans) {
        return toTuple(trans.getTransactionId(), trans.getStartTime());
    }

    public Tuple toTuple(CfdpTransactionId id, long startTime) {
        TupleDefinition td = CFDP.copy();
        ArrayList<Object> al = new ArrayList<>();
        al.add(startTime);
        al.add(id.getInitiatorEntity());
        al.add(id.getSequenceNumber());
        al.add(this.toByteArray());
        return new Tuple(td, al);
    }

    public static CfdpPacket fromTuple(Tuple tuple) {
        if (tuple.hasColumn("pdu")) {
            return CfdpPacket.getCFDPPacket(ByteBuffer.wrap((byte[]) (tuple.getColumn("pdu"))));
        } else {
            throw new IllegalStateException();
        }
    }

    public CfdpTransactionId getTransactionId() {
        return getHeader().getTransactionId();
    }

    private boolean crcValid() {
        throw new java.lang.UnsupportedOperationException("CFDP CRCs not supported");
    }

    // the buffer is assumed to be at the correct position
    protected abstract void writeCFDPPacket(ByteBuffer buffer);

    private void calculateAndAddCrc(ByteBuffer buffer) {
        throw new java.lang.UnsupportedOperationException("CFDP CRCs not supported");
    }
}
