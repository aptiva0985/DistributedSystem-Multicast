package distSysLab2.comm;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
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
    private volatile HashMap<String, LinkedList<MulticastMessage>> acks;
    private HashMap<String, GroupBean> groupList;

    public ReceiverThread(Socket socket, String configFile, ClockService clock,
                            ArrayList<RuleBean> recvRules, ArrayList<RuleBean> sendRules,
                            LinkedBlockingDeque<TimeStampMessage> recvQueue,
                            LinkedBlockingDeque<TimeStampMessage> recvDelayQueue,
                            HashMap<String, TimeStampMessage> holdBackQueue,
                            HashMap<String, LinkedList<MulticastMessage>> acks,
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
        this.acks = acks;
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
                		String keyOfAcks = null;
                		System.out.println(message.getData());
                		if(!message.getKind().equals("Ack")) {
                			keyOfAcks = ((MulticastMessage) message).getSrcGroup() + message.getSrc() + ((MulticastMessage) message).getNum();
                			if(!holdBackQueue.containsKey(keyOfAcks)) {
                				holdBackQueue.put(keyOfAcks, message);
                			}
                			if(holdBackQueue.get(keyOfAcks) == null) 
                				continue;
                		    String groupName = ((MulticastMessage) message).getSrcGroup();
                		    String localName = message.getDest();
                		    ArrayList<String> memberList = groupList.get(groupName).getMemberList();
                		    for(int i = 0; i < memberList.size(); i ++) {
                		    	if(!memberList.get(i).equals(localName)) {
                		    		MulticastMessage ackMsg = new MulticastMessage(memberList.get(i), "Ack", keyOfAcks);
                		    		ackMsg.setSrc(localName);
                		    		ackMsg.setTimeStamp(message.getTimeStamp());
                		    		ackMsg.setSeqNum(message.getSeqNum());
                		    		ackMsg.setSrcGroup(((MulticastMessage) message).getSrcGroup());
                		    		ackMsg.setNum(((MulticastMessage) message).getNum());
                		    		MessagePasser.getInstance().send(ackMsg);
                		    	}
                		    }
                			
                		}
                		else {
                			keyOfAcks = (String) message.getData();
                			System.out.println("33333 " + keyOfAcks);
                			if(acks.containsKey(keyOfAcks)) {
                				int i = 0;
                				for(; i < acks.get(keyOfAcks).size(); i ++) {
                					if(acks.get(keyOfAcks).get(i).getSrc().equals(message.getSrc())) {
                						break;
                					}
                				}
                				if(i == acks.get(keyOfAcks).size())
                					acks.get(keyOfAcks).add((MulticastMessage) message);
                			}
                			else {
                				LinkedList<MulticastMessage> temp = new LinkedList<MulticastMessage>();
                				temp.add((MulticastMessage) message);
                				acks.put(keyOfAcks, temp);
                				System.out.println("1111111111 " + acks.get(keyOfAcks).size());
                			}
                		}
                		
                		if((acks.get(keyOfAcks) != null) && (groupList.get(((MulticastMessage) message).getSrcGroup()).getMemberList().size() == (acks.get(keyOfAcks).size() + 1))) {
            				MulticastMessage multimessage = null;
            				if((holdBackQueue.containsKey(keyOfAcks))) {
            					multimessage = (MulticastMessage) holdBackQueue.get(keyOfAcks);
            				}
            				else
            					continue;
            				
            				message = multimessage;
            				acks.remove(keyOfAcks);
            				holdBackQueue.put(keyOfAcks, null);
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
