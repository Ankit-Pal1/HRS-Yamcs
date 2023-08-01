package com.hrs.mcs;

public class LeafMqttResponse{
    public String timestamp;
    public String payload;
    LeafMqttResponse(String timestamp, String payload){
        this.timestamp = timestamp;
        this.payload = payload;
    }

    public String getPayload() {
        return payload;
    }

    public String getTimestamp() {
        return timestamp;
    }
}
