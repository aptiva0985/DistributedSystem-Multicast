package distSysLab2.comm;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingDeque;

import distSysLab2.clock.ClockService;
import distSysLab2.clock.ClockService.ClockType;
import distSysLab2.message.MulticastMessage;
import distSysLab2.message.TimeStampMessage;
import distSysLab2.model.GroupBean;
import distSysLab2.model.NodeBean;
import distSysLab2.model.RuleBean;
import distSysLab2.model.RuleBean.RuleAction;

public class MessagePasser {
    private static MessagePasser instance;
    private ClockService clockServ;
    private LinkedBlockingDeque<TimeStampMessage> sendQueue = new LinkedBlockingDeque<TimeStampMessage>();
    private LinkedBlockingDeque<TimeStampMessage> sendDelayQueue = new LinkedBlockingDeque<TimeStampMessage>();
    private LinkedBlockingDeque<TimeStampMessage> recvQueue = new LinkedBlockingDeque<TimeStampMessage>();
    private LinkedBlockingDeque<TimeStampMessage> recvDelayQueue = new LinkedBlockingDeque<TimeStampMessage>();
    
    private HashMap<String, NodeBean> nodeList = new HashMap<String, NodeBean>();
    private HashMap<String, GroupBean> groupList = new HashMap<String, GroupBean>();
    private ArrayList<RuleBean> sendRules = new ArrayList<RuleBean>();
    private ArrayList<RuleBean> recvRules = new ArrayList<RuleBean>();
    
    private HashMap<String, TimeStampMessage> holdBackQueue = new HashMap<String, TimeStampMessage>();
    private HashMap<String, LinkedList<MulticastMessage>> sendMsgQueue =  new HashMap<String, LinkedList<MulticastMessage>>();
    private HashMap<String, LinkedList<MulticastMessage>> acks = new HashMap<String, LinkedList<MulticastMessage>>();
    private HashMap<String, Integer> sendCounter = new HashMap<String, Integer>();

    private String configFile;
    private String localName;
    private String loggerName;
    private String MD5Last;
    private int curSeqNum;

    private ListenerThread listener;
    private SenderThread sender;
    private ClockType clockType;

    /**
     * Actual constructor for MessagePasser
     *
     * @param configFile
     * @param localName
     */
    private MessagePasser(String configFile, String localName, String loggerName)
            throws UnknownHostException {
        this.loggerName = loggerName;
        this.localName = localName;
        this.configFile = configFile;
        this.curSeqNum = 0;

        ConfigParser.configurationFile = configFile;
        String type = ConfigParser.readClock();
        nodeList = ConfigParser.readConfig();
        groupList = ConfigParser.readGroup();
        sendRules = ConfigParser.readSendRules();
        recvRules = ConfigParser.readRecvRules();
        MD5Last = ConfigParser.getMD5Checksum(configFile);

        if(type.equalsIgnoreCase("VECTOR")) {
            clockType = ClockType.VECTOR;
        }
        else if(type.equalsIgnoreCase("LOGICAL")) {
            clockType = ClockType.LOGICAL;
        }

        clockServ = ClockService.getClockSerivce(clockType, localName, nodeList);

        if(nodeList.get(localName) == null) {
            System.err.println("The local name is incorrect.");
            System.exit(0);
        }
        else if (!InetAddress.getLocalHost().getHostAddress().toString().equals(nodeList.get(localName).getIp())) {
            System.err.println("Local ip do not match configuration file.");
            System.exit(0);
        }
        else {
            listener = new ListenerThread(nodeList.get(localName).getPort(), configFile,
                                            recvRules, sendRules, recvQueue, recvDelayQueue,clockServ, holdBackQueue, acks, groupList);
            sender = new SenderThread(sendQueue, nodeList);
        }

        System.out.println("Local status is: " + this.toString());
    }

    /**
     * Initialization for receive thread.
     */
    public synchronized void startListener() {
        Thread thread = new Thread(this.listener);
        thread.start();
    }

    /**
     * Initialization for send thread.
     */
    public synchronized void startSender() {
        Thread thread = new Thread(this.sender);
        thread.start();
    }

    /**
     * Singleton constructor for MessagePasser
     *
     * @param configuration_filename
     * @param local_name
     */
    public static synchronized MessagePasser getInstance(String configFileName,
                                                         String localName, String loggerName)
                                                         throws UnknownHostException {
        if (instance == null) {
            instance = new MessagePasser(configFileName, localName, loggerName);
        }
        return instance;
    }

    /**
     * Return existed instance of MessagePasser
     *
     * @return instance
     */
    public static MessagePasser getInstance() {
        return instance;
    }

    /**
     * Send a message. It can be a multicast message or a unicast one.
     *
     * @param message The message need to be sent.
     * @param willLog If this message need to be logged.
     */
    public void send(TimeStampMessage message) {
        // Set source and seq of the massage
        message.setSrc(localName);
        message.setSeqNum(curSeqNum++);
        this.getClockServ().updateTimeStampOnSend();
        message.setTimeStamp(clockServ.getCurTimeStamp());
        
        // If the message is a multicast one
        if(groupList.get(message.getDest()) != null) {
            GroupBean sendGroup = groupList.get(message.getDest());
            
            // Send the message to every group member
            for(String member : sendGroup.getMemberList()) {
                // Make a copy of the message and change the destination
                MulticastMessage actual = new MulticastMessage(message);
                actual.setDest(member);
                actual.setSrcGroup(sendGroup.getName());
                actual.setNum(sendCounter.get(sendGroup.getName()));
                
                // Update the counter for current receive group
                sendCounter.put(sendGroup.getName(), sendCounter.get(sendGroup.getName() + 1));
                
                checkRuleAndSend(actual);
            }
            
            // Setup timer for timeout
            AckChecker checker = new AckChecker(acks, (MulticastMessage) message, sendGroup.getMemberList());
            Thread thread = new Thread(checker);
            thread.start();
        }
        else {
            checkRuleAndSend(message);
        }
    }
    
    /**
     * Check message with send rule, then try to send the message. 
     * @param message The message need to be sent.
     * @param willLog If this message need to be logged.
     */
    public void checkRuleAndSend(TimeStampMessage message) {
        // Check if the configuration file has been changed.
        String MD5 = ConfigParser.getMD5Checksum(configFile);
        if (!MD5.equals(MD5Last)) {
            sendRules = ConfigParser.readSendRules();
            recvRules = ConfigParser.readRecvRules();
            MD5Last = MD5;
        }

        // Try to match a rule from the send rule list.
        RuleAction action = RuleAction.NONE;
        for (RuleBean rule : sendRules) {
            if (rule.isMatch(message)) {
                action = rule.getAction();
            }
        }

        // Do action according to the matched rule's type.
        switch (action) {
        case DROP:
            // Just drop this message.
            sendToLogger(message);
            break;

        case DUPLICATE:
            // Add this message into sendQueue.
            sendQueue.add(message);
            sendToLogger(message);
            // Add a duplicate message into sendQueue.
            TimeStampMessage copy = (TimeStampMessage) message.copyOf();
            copy.setDuplicate(true);
            sendQueue.add(copy);
            sendToLogger(copy);
            sendQueue.addAll(sendDelayQueue);
            sendDelayQueue.clear();
            break;

        case DELAY:
            // Add this message into delayQueue
            sendDelayQueue.add(message);
            sendToLogger(message);
            break;

        case NONE:
        default:
            // Add this message into sendQueue
            sendQueue.add(message);
            sendToLogger(message);
            sendQueue.addAll(sendDelayQueue);
            sendDelayQueue.clear();
        }
    }
    

    /**
     * For message that need to be logged, send it to the logger.
     * @param msg The message that will be logged
     */
    public void sendToLogger(TimeStampMessage msg) {
        if(nodeList.get(loggerName) == null) {
            System.err.println("You have not assigned a valid logger.");
            return;
        }

        // Build a wrapper message to make the original message as its data field.
        TimeStampMessage wrapper = new TimeStampMessage(loggerName, "LOG", msg);
        wrapper.setSrc(localName);
        wrapper.setSeqNum(curSeqNum++);
        wrapper.setTimeStamp(msg.getTimeStamp());

        sendQueue.add(wrapper);
    }

    /**
     * Deliver message from the receive queue
     * @param willLog
     *
     * @return A message
     */
    public TimeStampMessage receive() {
        TimeStampMessage message = null;
        synchronized (recvQueue) {
            if (!recvQueue.isEmpty()) {
                message = recvQueue.poll();
            }
        }

        return message;
    }

    public void teminate() throws IOException {
        listener.teminate();

        sender.teminate();
    }

    public HashMap<String, NodeBean> getNodeList() {
        return nodeList;
    }
    
    public HashMap<String, GroupBean> getGroupList() {
        return groupList;
    }

    public ClockService getClockServ() {
        return clockServ;
    }

    @Override
    public String toString() {
        return "MessagePasser [configFile=" + configFile + ", localName=" + localName
                + ", listenSocket=" + listener.toString() + "]";
    }
}