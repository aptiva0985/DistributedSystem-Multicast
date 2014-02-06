package distSysLab2.logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingDeque;

import distSysLab2.comm.ConfigParser;
import distSysLab2.message.TimeStampMessage;
import distSysLab2.model.NodeBean;

public class Logger {
    private static Logger instance;

    private HashMap<String, NodeBean> nodeList = new HashMap<String, NodeBean>();
    private LinkedBlockingDeque<TimeStampMessage> recvQueue = new LinkedBlockingDeque<TimeStampMessage>();
    private ArrayList<TimeStampMessage> logList = new ArrayList<TimeStampMessage>();
    private String localName;

    public Logger(String configFile, String localName) throws UnknownHostException {
        ConfigParser.configurationFile = configFile;
        this.localName = localName;

        nodeList = ConfigParser.readConfig();

        if(nodeList.get(localName) == null) {
            System.err.println("The local name is incorrect.");
            System.exit(0);
        }
        else if(!InetAddress.getLocalHost().getHostAddress().toString()
                            .equals(nodeList.get(localName).getIp())) {
            System.err.println("Local ip do not match configuration file.");
            System.exit(0);
        }
        else {
            startListener();

            startUIThread();
        }
    }

    public static Logger getInstance(String configFile, String localName) throws UnknownHostException {
        if (instance == null) {
            instance = new Logger(configFile, localName);
        }
        return instance;
    }

    public static Logger getInstance() {
        return instance;
    }

    public LinkedBlockingDeque<TimeStampMessage> getRecvQueue() {
        return recvQueue;
    }

    /**
     * Initialization for listener thread.
     */
    private synchronized void startListener() {
        // set up listener thread to build connection with other nodes
        int port = nodeList.get(localName).getPort();
        Thread thread = new Thread(new LoggerListenerThread(port));
        thread.start();
    }

    /**
     * Initialization for UI thread.
     */
    private synchronized void startUIThread() {
        // set up user thread to receive user input
        Thread thread = new Thread(new LoggerUIThread());
        thread.start();
    }

    /**
     * Deliver message from the receive queue
     *
     * @return A message
     */
    public TimeStampMessage receive() {
        TimeStampMessage message = null;
        synchronized (recvQueue) {
            if(!recvQueue.isEmpty()) {
                message = recvQueue.poll();
            }
        }

        return message;
    }

    /**
     * Show all the message that has been received.
     *
     * @return ArrayList<TimeStampMessage>
     */
    public ArrayList<TimeStampMessage> showMessages() {
        while(recvQueue.size() > 0) {
            this.logList.add(recvQueue.poll());
        }

        Collections.sort(logList);
        return logList;
    }
}
