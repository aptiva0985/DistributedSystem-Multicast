package distSysLab2.comm;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingDeque;

import distSysLab2.message.MessageKey;
import distSysLab2.message.MulticastMessage;
import distSysLab2.message.TimeStampMessage;
import distSysLab2.model.GroupBean;
import distSysLab2.model.RuleBean;
import distSysLab2.model.RuleBean.RuleAction;
import distSysLab2.clock.ClockService;

public class ReceiverThread implements Runnable {
    private Socket socket;
    private ObjectInputStream in;
    private ArrayList<RuleBean> recvRules;
    private ArrayList<RuleBean> sendRules;
    private ClockService clock;
    private LinkedBlockingDeque<TimeStampMessage> recvQueue;
    private LinkedBlockingDeque<TimeStampMessage> recvDelayQueue;
    private String configFile;
    private String MD5Last;
    private HashMap<MessageKey, MulticastMessage> holdBackQueue;
    private volatile HashMap<MessageKey, HashSet<String>> acks;
    private HashMap<String, Integer> orderCounter;
    private HashMap<String, GroupBean> groupList;

    public ReceiverThread(Socket socket, String configFile, ClockService clock,
                            ArrayList<RuleBean> recvRules, ArrayList<RuleBean> sendRules,
                            LinkedBlockingDeque<TimeStampMessage> recvQueue,
                            LinkedBlockingDeque<TimeStampMessage> recvDelayQueue,
                            HashMap<MessageKey, MulticastMessage> holdBackQueue,
                            HashMap<MessageKey, HashSet<String>> acks,
                            HashMap<String, GroupBean> groupList,
                            HashMap<String, Integer> orderCounter) {
        this.socket = socket;
        this.recvQueue = recvQueue;
        this.in = null;
        this.clock = clock;
        this.recvDelayQueue = recvDelayQueue;
        this.recvRules = recvRules;
        this.sendRules = sendRules;
        this.configFile = configFile;
        MD5Last = "";
        this.holdBackQueue = holdBackQueue;
        this.acks = MessagePasser.getInstance().getAcks();
        this.groupList = groupList;
        this.orderCounter = orderCounter;
    }

    @Override
    public void run() {
        try {
            while(true) {
                TimeStampMessage message = null;
                String MD5 = ConfigParser.getMD5Checksum(configFile);
                if (!MD5.equals(MD5Last)) {
                    sendRules = ConfigParser.readSendRules();
                    recvRules = ConfigParser.readRecvRules();
                    MD5Last = MD5;
                }

                in = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
                if((message = (TimeStampMessage) (in.readObject())) != null) {
                    // Try to match a rule and act corresponding
                    // The match procedure should be in the listener thread
                	
                	if(message instanceof MulticastMessage) {
                	    processMulti((MulticastMessage) message);
                	}
                	else {
                	    checkRuleAndPut(message);
                	}
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    private void processMulti(MulticastMessage multiMsg) {
        String destGroup = multiMsg.getSrcGroup();
        ArrayList<String> memberList = groupList.get(destGroup).getMemberList();
        MessageKey curKey = new MessageKey();
        
        // If we receive a real multicast message
        if(!multiMsg.getKind().equals("Ack")) {
            String localName = multiMsg.getDest();
            System.out.println("Get a multicast message, from " +
                               multiMsg.getSrc() + " destGroup: " + destGroup + multiMsg);
            
            curKey.setDestGroup(multiMsg.getSrcGroup());
            curKey.setSrc(multiMsg.getSrc());
            curKey.setNum(multiMsg.getNum());
            
            // If it is a new message, add it to holdBackQueue
            if(!holdBackQueue.containsKey(curKey)) {
                holdBackQueue.put(curKey, multiMsg);
            }
            
            // Send Ack message to every group member, exclude itself
            for(String member : memberList) {
                if(!member.equals(localName)) {
                    MulticastMessage ackMsg = new MulticastMessage(member, "Ack", curKey);
                    ackMsg.setSrc(localName);
                    ackMsg.setTimeStamp(multiMsg.getTimeStamp());
                    ackMsg.setSeqNum(multiMsg.getSeqNum());
                    ackMsg.setSrcGroup(((MulticastMessage) multiMsg).getSrcGroup());
                    ackMsg.setNum(((MulticastMessage) multiMsg).getNum());
                    System.out.println("Send Ack message from " + ackMsg.getSrc() + ", to: " + ackMsg.getDest());
                    MessagePasser.getInstance().checkRuleAndSend(ackMsg);
                }
            }
        }
        else {
            System.out.println("Get Ack message, from " + multiMsg.getSrc() + " to: " + multiMsg.getDest() + multiMsg);
            
            curKey = (MessageKey) multiMsg.getData();
            if(acks.containsKey(curKey)) {
                acks.get(curKey).add(multiMsg.getSrc());
            }
            else {
                HashSet<String> tmp = new HashSet<String>();
                tmp.add(multiMsg.getSrc());
                acks.put(curKey, tmp);
            }
        }
        
        // If we get enough ack for this message
        if((acks.get(curKey) != null) &&
           (acks.get(curKey).size() == memberList.size() - 1) &&
           (holdBackQueue.containsKey(curKey))) {
            //String msgKey = curKey.getSrc() + curKey.getDestGroup();
            deliver(curKey);
        } 
    }
    
    private void deliver(MessageKey curKey) {
        String msgKey = curKey.getSrc() + curKey.getDestGroup();
        if(orderCounter.get(msgKey) == null) {
            orderCounter.put(msgKey, 0);
        }
        
        if(orderCounter.get(msgKey) + 1 == curKey.getNum()) {
            while(holdBackQueue.containsKey(curKey)) {
                MulticastMessage multiMsg = holdBackQueue.get(curKey);
                orderCounter.put(msgKey, multiMsg.getNum());
                holdBackQueue.remove(curKey);
                checkRuleAndPut(multiMsg);
                curKey.setNum(curKey.getNum() + 1);
            }
        }
    }
    
    private void checkRuleAndPut(TimeStampMessage message) {
        RuleAction action = RuleAction.NONE;
        for (RuleBean rule : recvRules) {
            if (rule.isMatch(message)) {
                action = rule.getAction();
            }
        }

        // Update local time stamp when there is new incoming message.
        synchronized (clock) {
            clock.updateTimeStampOnReceive(message.getTimeStamp());
        }
        synchronized(recvQueue) {
            // Do action according to the matched rule's type.
            // if one non-delay message comes(even with drop kind?),
            // then all messages in delay queue go to normal queue
            switch (action) {
                case DROP:
                    // Just drop this message.
                    break;
                case DUPLICATE:
                    // Add this message into recvQueue.
                    recvQueue.add(message);
                    // Add a duplicate message into recvQueue.
                    TimeStampMessage copy = (TimeStampMessage) message.copyOf();
                    copy.setDuplicate(true);
                    recvQueue.add(copy);
                    recvQueue.addAll(recvDelayQueue);
                    recvDelayQueue.clear();
                    break;
                case DELAY:
                    // Add this message into delayQueue
                    recvDelayQueue.add(message);
                    break;
                case NONE:
                default:
                    recvQueue.add(message);
                    recvQueue.addAll(recvDelayQueue);
                    recvDelayQueue.clear();
            }
        }
    }

    public void teminate() throws IOException {
        socket.close();
    }
}
