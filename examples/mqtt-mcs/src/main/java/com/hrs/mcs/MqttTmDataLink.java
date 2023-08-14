package com.hrs.mcs;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.yamcs.ConfigurationException;
import org.yamcs.Spec;
import org.yamcs.TmPacket;
import org.yamcs.YConfiguration;
import org.yamcs.tctm.AbstractTmDataLink;

import javax.net.ssl.SSLSocketFactory;
import java.util.Arrays;

public class MqttTmDataLink extends AbstractTmDataLink implements MqttCallback {
    private MqttClient mqttclient;
    long reconnectionDelay;
    private String username;
    private char[] password;
    private String brokerUrl;
    private String clientId;
    private String downlinkTopic;
    private int qos;

    @Override
    protected Status connectionStatus() {
        return mqttclient.isConnected() ? Status.OK : Status.UNAVAIL;
    }

    @Override
    public Spec getSpec() {
        Spec spec = getDefaultSpec();
        spec.addOption("url", Spec.OptionType.STRING).withRequired(true)
                .withDescription("The URL to connect to the MQTT server.");

        spec.addOption("prefix", Spec.OptionType.STRING)
                .withDescription("prefix for client for generating clientId to connect to the MQTT server");

        spec.addOption("downlink", Spec.OptionType.STRING).withRequired(true)
                .withDescription("downlink topic for sending message to the MQTT server");

        spec.addOption("qos", Spec.OptionType.INTEGER)
                .withDescription("qos for uplink, Default is 0");

        spec.addOption("username", Spec.OptionType.STRING)
                .withDescription("Username to connect to the MQTT server");

        spec.addOption("password", Spec.OptionType.STRING).withSecret(true)
                .withDescription("Password to connect to the MQTT server");
        return spec;
    }

    @Override
    public void init(String yamcsInstance, String linkName, YConfiguration config) throws ConfigurationException {
        super.init(yamcsInstance, linkName, config);
        this.reconnectionDelay = config.getLong("reconnectionDelay", 5000);
        this.brokerUrl = config.getString("url");
        this.clientId = config.getString("prefix", "") + MqttClient.generateClientId();
        this.downlinkTopic = config.getString("downlink");
        this.qos = config.getInt("qos");

        try {
            MemoryPersistence persistence = new MemoryPersistence();
            mqttclient = new MqttClient(brokerUrl, clientId, persistence);
            mqttclient.setCallback(this);
        } catch (MqttException e) {
            log.error("Failed to build MQTT broker", e);
        }

        if (config.containsKey("username")) {
            if (config.containsKey("password")) {
                username = config.getString("username");
                password = config.getString("password").toCharArray();
            } else {
                throw new ConfigurationException("Username provided with no password");
            }
        } else if (config.containsKey("password")) {
            throw new ConfigurationException("Password provided with no username");
        }
    }

    protected synchronized void openConnection() {
        if (mqttclient != null && mqttclient.isConnected()) {
            // we have to protect against double connection because there might be a timer and a user action that causes
            // it to come in here
            return;
        }
        connectToBroker();

    }

    private void connectToBroker() {
        try {
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);

            if (username != null) {
                connOpts.setUserName(username);
                connOpts.setPassword(password);
            }

            //enable TLS
            connOpts.setSocketFactory(SSLSocketFactory.getDefault());

            mqttclient.connect(connOpts);
            log.info("Connected to MQTT broker : " + mqttclient.getServerURI());
            mqttclient.subscribe(downlinkTopic);
        } catch (MqttException cause) {
            log.warn("Connection to upstream MQTT server failed", cause);
            eventProducer.sendWarning("Connection to upstream MQTT failed: " + cause);
        }
    }


    @Override
    protected void doStart() {
        if (!isDisabled()) {
            openConnection();
        }
        notifyStarted();
    }

    @Override
    protected void doStop() {
        try {
            if (mqttclient != null && mqttclient.isConnected()) {
                mqttclient.disconnect();
                log.info("disconnected from MQTT: %s", mqttclient.getServerURI());
                mqttclient.close();
            }
        } catch (MqttException e) {
            log.error("Failed to disconnect from Mqtt server", e);
        }
        notifyStopped();

    }

    @Override
    public String getDetailedStatus() {
        if (isDisabled()) {
            return String.format("DISABLED (should connect to %s:%s)", brokerUrl, downlinkTopic);
        }
        if (mqttclient != null && mqttclient.isConnected()) {
            return String.format("OK, connected to %s:%s)", brokerUrl, downlinkTopic);
        } else {
            return String.format("Not connected to %s:%s)", brokerUrl, downlinkTopic);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.error("Connection lost to mqtt broker, retrying...");
        connectToBroker();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        String jsonPayload = new String(message.getPayload());
        var gson = new Gson();
        var leafResponse = gson.fromJson(jsonPayload, LeafMqttResponse.class);
        String payloadHex = leafResponse.getPayload();
        log.info("Payload Hex: " + payloadHex);
        try{
        byte[] packetData = ByteArrayToBinary.hexToBytes(payloadHex);


        log.info("received message from mqtt broker against topic %s : " + Arrays.toString(packetData) + "\n at time : " + leafResponse.timestamp );

        String messageString = new String(Arrays.copyOfRange(packetData,0,packetData.length));

        log.info("actual message from leaf : " +  messageString);


        // Now create a TmPacket object using the received packet data
        TmPacket tmPacket = new TmPacket(timeService.getMissionTime(), packetData);
        tmPacket.setEarthReceptionTime(timeService.getHresMissionTime());

        // Process the packet using the packet preprocessor
        TmPacket processedPacket = packetPreprocessor.process(tmPacket);


        if (processedPacket != null) {
            processPacket(processedPacket);
        }
        }catch (Exception e){
            log.error("error i parsing hex", e);
        }

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}
