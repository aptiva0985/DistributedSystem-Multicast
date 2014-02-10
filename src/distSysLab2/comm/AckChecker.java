package distSysLab2.comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import distSysLab2.message.MessageKey;
import distSysLab2.message.MulticastMessage;

public class AckChecker implements Runnable {

    private MulticastMessage message;
    private ArrayList<String> memberList;
    private volatile HashMap<MessageKey, HashSet<String>> acks;

    public AckChecker(HashMap<MessageKey, HashSet<String>> acks,
                      MulticastMessage message, ArrayList<String> memberList) {
        this.acks = MessagePasser.getInstance().getAcks();
        this.message = message;
        this.memberList = memberList;
    }

    @Override
    public void run() {
        try {
            // Sleep 2s for incoming Acks
            Thread.sleep(2000);

            MessageKey key = new MessageKey();
            key.setDestGroup(message.getSrcGroup());
            key.setSrc(message.getSrc());
            key.setNum(message.getNum());

            // Check if we get enought Acks
            if(acks.get(key).size() == memberList.size() - 1) {
                //System.out.println("Got enough Ack for message: " + key);
                return;
            }
            else {
                while(true) {
                    // Sleep 5s before rechecking
                    Thread.sleep(5000);

                    if(acks.get(key).size() == memberList.size() - 1) {
                        //System.out.println("Got enough Ack for message: " + key);
                        return;
                    }
                    else {
                        System.out.println("Timeout for message: " + key + " Resend.");

                        // Do resend
                        for(String member : memberList) {
                            MulticastMessage resend = new MulticastMessage(member,
                                                                           message.getKind(),
                                                                           message.getData());
                            resend.setDuplicate(message.getDuplicate());
                            resend.setNum(message.getNum());
                            resend.setSeqNum(message.getSeqNum());
                            resend.setSrc(message.getSrc());
                            resend.setSrcGroup(message.getSrcGroup());
                            resend.setTimeStamp(message.getTimeStamp());
                            MessagePasser.getInstance().checkRuleAndSend(resend);
                            //System.out.println("Resend to " + resend.getDest() + resend);
                        }
                    }
                }
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
