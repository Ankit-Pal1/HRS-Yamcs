package com.hrs.mcs;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.yamcs.ConfigurationException;
import org.yamcs.Spec;
import org.yamcs.YConfiguration;
import org.yamcs.tctm.AbstractTmDataLink;

import javax.net.ssl.SSLSocketFactory;

public class MqttTmDataLink extends AbstractTmDataLink implements  MqttCallback {
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
        this.downlinkTopic = config.getString("downlinkTopic");
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
            log.info("Connected to MQTT broker : " + mqttclient);
            mqttclient.subscribe(downlinkTopic);
        } catch (MqttException cause) {
            log.warn("Connection to upstream MQTT server failed", cause);
            eventProducer.sendWarning("Connection to upstream MQTT failed: " + cause);
        }
    }


    @Override
    protected void doStart() {
        if(!isDisabled()) {
            openConnection();
        }
        notifyStarted();
    }

    @Override
    protected void doStop() {
        try{
            if (mqttclient != null && mqttclient.isConnected()) {
                mqttclient.disconnect();
                log.info("disconnected from MQTT", mqttclient);
                mqttclient.close();
                notifyStopped();
            }
        } catch (MqttException e) {
            log.error("Failed to disconnect from Mqtt server", e);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.error("Connect");
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {

    }
}
