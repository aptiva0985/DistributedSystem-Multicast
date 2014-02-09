package distSysLab2.comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import distSysLab2.message.MulticastMessage;

public class AckChecker implements Runnable {
    
    private MulticastMessage message;
    private ArrayList<String> memberList;
    private volatile HashMap<String, HashSet<String>> acks;

    public AckChecker(HashMap<String, HashSet<String>> acks,
                      MulticastMessage message, ArrayList<String> memberList) {
        this.acks = MessagePasser.getInstance().getAcks();
        this.message = message;
        this.memberList = memberList;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(2000);
            
            String key = message.getSrcGroup() + message.getSrc() + message.getNum();
            if(acks.get(key).size() == memberList.size() - 1) {
                System.out.println("Got enough Ack for message: " + key);
                return;
            }
            else {
                while(true) {
                    Thread.sleep(5000);
                    
                    if(acks.get(key).size() == memberList.size() - 1) {
                        System.out.println("Got enough Ack for message: " + key);
                        return;
                    }
                    else {
                        System.out.println("Timeout for message: " + key);
                        
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
                            System.out.println("Resend to " + resend.getDest() + resend);
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
