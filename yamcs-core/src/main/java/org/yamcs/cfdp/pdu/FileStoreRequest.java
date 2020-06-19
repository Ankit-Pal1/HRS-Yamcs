package org.yamcs.cfdp.pdu;

import java.nio.ByteBuffer;

public class FileStoreRequest {
    private ActionCode actionCode;
    private LV firstFileName;
    private LV secondFileName;

    public FileStoreRequest(ActionCode actionCode, LV firstFileName, LV secondFileName) {
        this.actionCode = actionCode;
        this.firstFileName = firstFileName;
        this.secondFileName = secondFileName;
    }

    public FileStoreRequest(ActionCode actionCode, LV firstFileName) {
        this(actionCode, firstFileName, null);
    }

    public ActionCode getActionCode() {
        return this.actionCode;
    }

    public LV getFirstFileName() {
        return this.firstFileName;
    }

    public LV getSecondFileName() {
        return this.secondFileName;
    }

    // TODO merge this with fromTLV
    private static FileStoreRequest readFileStoreRequest(ByteBuffer buffer) {
        ActionCode c = ActionCode.readActionCode(buffer);
        return (c.hasSecondFileName()
                ? new FileStoreRequest(c, LV.readLV(buffer), LV.readLV(buffer))
                : new FileStoreRequest(c, LV.readLV(buffer)));
    }

    public static FileStoreRequest fromTLV(TLV tlv) {
        return readFileStoreRequest(ByteBuffer.wrap(tlv.getValue()));
    }

    public TLV toTLV() {
        if (secondFileName != null) {
            return new TLV(TLV.TYPE_FileStoreRequest,
                    ByteBuffer
                            .allocate(1
                                    + firstFileName.getValue().length + 1
                                    + secondFileName.getValue().length + 1)
                            .put((byte) (actionCode.getCode() << 4))
                            .put((byte) firstFileName.getValue().length)
                            .put(firstFileName.getValue())
                            .put((byte) secondFileName.getValue().length)
                            .put(secondFileName.getValue())
                            .array());
        } else {
            return new TLV(TLV.TYPE_FileStoreRequest,
                    ByteBuffer
                            .allocate(1
                                    + firstFileName.getValue().length + 1)
                            .put((byte) (actionCode.getCode() << 4))
                            .put((byte) firstFileName.getValue().length)
                            .put(firstFileName.getValue())
                            .array());
        }
    }
}
