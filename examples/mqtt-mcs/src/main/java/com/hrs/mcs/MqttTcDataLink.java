package com.hrs.mcs;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.yamcs.ConfigurationException;
import org.yamcs.Spec;
import org.yamcs.YConfiguration;
import org.yamcs.commanding.PreparedCommand;
import org.yamcs.tctm.AbstractThreadedTcDataLink;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.util.Arrays;

/**
 * Send raw packet over mqtt
 *
 * @author InnobitSystems.com
 */
public class MqttTcDataLink extends AbstractThreadedTcDataLink implements MqttCallback {
    private MqttClient mqttclient;
    long reconnectionDelay;
    private String username;
    private char[] password;
    private String brokerUrl;
    private String clientId;
    private String uplinkTopic;
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

        spec.addOption("uplink", Spec.OptionType.STRING).withRequired(true)
                .withDescription("uplink topic for sending message to the MQTT server");

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
        this.uplinkTopic = config.getString("uplink");
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

    @Override
    protected void uplinkCommand(PreparedCommand pc) throws IOException {
        byte[] binary = postprocess(pc);
        if (binary == null) {
            return;
        }
        log.info("sent data to uplink : "+ Arrays.toString(binary) + "\n and binary " + ByteArrayToBinary.byteArrayToBinary(binary));
        try {
            mqttclient.publish(uplinkTopic, binary, qos, false);
        } catch (MqttException e) {
            throw new IOException(e);
        }
        dataCount.getAndIncrement();
        ackCommand(pc.getCommandId());
    }

    protected synchronized boolean openConnection() {
        if (mqttclient != null && mqttclient.isConnected()) {
            // we have to protect against double connection because there might be a timer and a user action that causes
            // it to come in here
            return true;
        }

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
            return true;
        } catch (MqttException cause) {
            log.warn("Connection to upstream MQTT server failed", cause);
            eventProducer.sendWarning("Connection to upstream MQTT failed: " + cause);
        }
        return false;
    }

    @Override
    protected void startUp() throws Exception {
        if (!isDisabled()) {
            openConnection();
        }
    }

    @Override
    protected void shutDown() throws Exception {
        try {
            if (mqttclient != null && mqttclient.isConnected()) {
                mqttclient.disconnect();
                log.info("disconnected from MQTT: %s", mqttclient.getServerURI());
                mqttclient.close();
            }
        } catch (MqttException e) {
            log.error("Failed to disconnect from Mqtt server", e);
        }
    }

    @Override
    public String getDetailedStatus() {
        if (isDisabled()) {
            return String.format("DISABLED (should connect to %s:%s)", brokerUrl,uplinkTopic);
        }
        if (mqttclient != null && mqttclient.isConnected()) {
            return String.format("OK, connected to %s:%s)", brokerUrl,uplinkTopic);
        } else {
            return String.format("Not connected to %s:%s)", brokerUrl,uplinkTopic);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {

    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        log.info("message delivered successfully for token", token.toString());
    }
}