package distSysLab2.comm;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingDeque;

import distSysLab2.message.TimeStampMessage;
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

    public ReceiverThread(Socket socket, String configFile, ClockService clock,
                            ArrayList<RuleBean> recvRules, ArrayList<RuleBean> sendRules,
                            LinkedBlockingDeque<TimeStampMessage> recvQueue,
                            LinkedBlockingDeque<TimeStampMessage> recvDelayQueue) {
        this.socket = socket;
        this.recvQueue = recvQueue;
        this.in = null;
        this.clock = clock;
        this.recvDelayQueue = recvDelayQueue;
        this.recvRules = recvRules;
        this.sendRules = sendRules;
        this.configFile = configFile;
        MD5Last = "";
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
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void teminate() throws IOException {
        socket.close();
    }
}
