package distSysLab2.comm;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingDeque;

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
    private HashMap<String, TimeStampMessage> holdBackQueue;
    private volatile HashMap<String, HashSet<String>> acks;
    private HashMap<String, GroupBean> groupList;

    public ReceiverThread(Socket socket, String configFile, ClockService clock,
                            ArrayList<RuleBean> recvRules, ArrayList<RuleBean> sendRules,
                            LinkedBlockingDeque<TimeStampMessage> recvQueue,
                            LinkedBlockingDeque<TimeStampMessage> recvDelayQueue,
                            HashMap<String, TimeStampMessage> holdBackQueue,
                            HashMap<String, HashSet<String>> acks,
                            HashMap<String, GroupBean> groupList) {
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
                	    MulticastMessage multiMsg = (MulticastMessage) message;
                	    String destGroup = multiMsg.getSrcGroup();
                        String localName = multiMsg.getDest();
                        ArrayList<String> memberList = groupList.get(destGroup).getMemberList();
                	    String keyOfAcks = destGroup + multiMsg.getSrc() + multiMsg.getNum();
                	    
                	    // If we receive a real multicast message
                	    if(!message.getKind().equals("Ack")) {
                	        System.out.println("Get a multicast message, from " +
                	                           multiMsg.getSrc() + " destGroup: " + destGroup + multiMsg);
                	        
                	        // If it is a new message, add it to holdBackQueue
                	        if(!holdBackQueue.containsKey(keyOfAcks)) {
                                holdBackQueue.put(keyOfAcks, multiMsg);
                            }
                	        
                	        // Send Ack message to every group member, exclude itself
                	        for(String member : memberList) {
                	            if(!member.equals(localName)) {
                	                MulticastMessage ackMsg = new MulticastMessage(member, "Ack", keyOfAcks);
                                    ackMsg.setSrc(localName);
                                    ackMsg.setTimeStamp(message.getTimeStamp());
                                    ackMsg.setSeqNum(message.getSeqNum());
                                    ackMsg.setSrcGroup(((MulticastMessage) message).getSrcGroup());
                                    ackMsg.setNum(((MulticastMessage) message).getNum());
                                    System.out.println("Send Ack message from " + ackMsg.getSrc() + ", to: " + ackMsg.getDest());
                                    MessagePasser.getInstance().checkRuleAndSend(ackMsg);
                	            }
                	        }
                	    }
                	    else {
                	        System.out.println("Get Ack message, from " + multiMsg.getSrc() + " to: " + multiMsg.getDest() + multiMsg);
                	        
                	        keyOfAcks = (String) message.getData();
                	        if(acks.containsKey(keyOfAcks)) {
                	            acks.get(keyOfAcks).add(message.getSrc());
                	        }
                	        else {
                	            HashSet<String> tmp = new HashSet<String>();
                	            tmp.add(message.getSrc());
                	            acks.put(keyOfAcks, tmp);
                	        }
                	    }
                	    
                	    // If we get enough ack for this message
                	    if((acks.get(keyOfAcks) != null) &&
                	       (acks.get(keyOfAcks).size() == memberList.size() - 1)) {
                	        System.out.println("Got enough Ack for message: " + keyOfAcks);
                	        
                	        if((holdBackQueue.containsKey(keyOfAcks))) {
                	            System.out.println("Should deliver now.");
                                // TODO Deliver
                	            message = holdBackQueue.get(keyOfAcks);
                	            holdBackQueue.remove(keyOfAcks);
                            }
                	        else {
                	            continue;
                	        }
                	    }
                	    else {
                	        continue;
                	    }
                	}
          
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
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void teminate() throws IOException {
        socket.close();
    }
}
