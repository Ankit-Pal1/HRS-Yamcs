package org.yamcs.cfdp.pdu;

public class MessageToUser {

    private byte[] message;

    public MessageToUser(byte[] message) {
        this.message = message;
    }

    public byte[] getMessage() {
        return this.message;
    }

    public static MessageToUser fromTLV(TLV tlv) {
        return new MessageToUser(tlv.getValue());
    }

    public TLV toTLV() {
        return new TLV(TLV.TYPE_MESSAGE_TO_USER, getMessage());
    }
}
