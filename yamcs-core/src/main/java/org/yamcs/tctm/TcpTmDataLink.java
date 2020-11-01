package org.yamcs.tctm;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.yamcs.ConfigurationException;
import org.yamcs.TmPacket;
import org.yamcs.YConfiguration;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.utils.YObjectLoader;

public class TcpTmDataLink extends AbstractTmDataLink implements Runnable {

    protected Socket tmSocket;
    protected String host;
    protected int port;
    protected long initialDelay;

    ParameterValue svConnectionStatus;

    String packetInputStreamClassName;
    YConfiguration packetInputStreamArgs;
    PacketInputStream packetInputStream;

    @Override
    public void init(String instance, String name, YConfiguration config) throws ConfigurationException {
        super.init(instance, name, config);
        if (config.containsKey("tmHost")) { // this is when the config is specified in tcp.yaml
            host = config.getString("tmHost");
            port = config.getInt("tmPort");
        } else {
            host = config.getString("host");
            port = config.getInt("port");
        }
        initialDelay = config.getLong("initialDelay", -1);
        if (config.containsKey("packetInputStreamClassName")) {
            this.packetInputStreamClassName = config.getString("packetInputStreamClassName");
            this.packetInputStreamArgs = config.getConfig("packetInputStreamArgs");
        } else {
            this.packetInputStreamClassName = CcsdsPacketInputStream.class.getName();
            this.packetInputStreamArgs = YConfiguration.emptyConfig();
        }
        
    }

    protected void openSocket() throws IOException {
        InetAddress address = InetAddress.getByName(host);
        tmSocket = new Socket();
        tmSocket.setKeepAlive(true);
        tmSocket.connect(new InetSocketAddress(address, port), 1000);
        try {
            packetInputStream = YObjectLoader.loadObject(packetInputStreamClassName);
        } catch (ConfigurationException e) {
            log.error("Cannot instantiate the packetInput stream", e);
            throw e;
        }
        packetInputStream.init(tmSocket.getInputStream(), packetInputStreamArgs);
    }

    @Override
    public void doStart() {
        if (!isDisabled()) {
            new Thread(this).start();
        }
        notifyStarted();
    }

    @Override
    public void doStop() {
        if (tmSocket != null) {
            try {
                tmSocket.close();
            } catch (IOException e) {
                log.warn("Exception got when closing the tm socket:", e);
            }
            tmSocket = null;
        }
        notifyStopped();
    }

    @Override
    public void run() {
        if (initialDelay > 0) {
            try {
                Thread.sleep(initialDelay);
                initialDelay = -1;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        while (isRunningAndEnabled()) {
            TmPacket tmpkt = getNextPacket();
            if (tmpkt == null) {
                break;
            }
            processPacket(tmpkt);
        }
    }

    public TmPacket getNextPacket() {
        TmPacket pwt = null;
        while (isRunningAndEnabled()) {
            try {
                if (tmSocket == null) {
                    openSocket();
                    log.info("Link established to {}:{}", host, port);
                }
                byte[] packet = packetInputStream.readPacket();
                updateStats(packet.length);
                pwt = packetPreprocessor.process(new TmPacket(timeService.getMissionTime(), packet));
                if (pwt != null) {
                    break;
                }
            } catch (EOFException e) {
                log.warn("TM Connection closed");
                tmSocket = null;
            } catch (IOException e) {
                if (isRunningAndEnabled()) {
                    String exc = (e instanceof ConnectException) ? ((ConnectException) e).getMessage() : e.toString();
                    log.info("Cannot open or read TM socket {}:{} {}'. Retrying in 10s", host, port, exc);
                }
                try {
                    tmSocket.close();
                } catch (Exception e2) {
                }
                tmSocket = null;
                for (int i = 0; i < 10; i++) {
                    if (!isRunningAndEnabled()) {
                        break;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }
            } catch (PacketTooLongException e) {
                log.warn(e.toString());
                try {
                    tmSocket.close();
                } catch (Exception e2) {
                }
                tmSocket = null;
            }
        }
        return pwt;
    }

    @Override
    public void doDisable() {
        if (tmSocket != null) {
            try {
                tmSocket.close();
            } catch (IOException e) {
                log.warn("Exception got when closing the tm socket:", e);
            }
            tmSocket = null;
        }
    }

    @Override
    public void doEnable() {
        new Thread(this).start();
    }

    @Override
    public String getDetailedStatus() {
        if (isDisabled()) {
            return String.format("DISABLED (should connect to %s:%d)", host, port);
        }
        if (tmSocket == null) {
            return String.format("Not connected to %s:%d", host, port);
        } else {
            return String.format("OK, connected to %s:%d, received %d packets", host, port, packetCount.get());
        }
    }

    @Override
    protected Status connectionStatus() {
        return (tmSocket == null) ? Status.UNAVAIL : Status.OK;
    }
}
