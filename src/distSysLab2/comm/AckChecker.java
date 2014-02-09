package distSysLab2.comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import distSysLab2.message.MulticastMessage;

public class AckChecker implements Runnable {
    
    private MulticastMessage message;
    private ArrayList<String> memberList;
    private volatile HashMap<String, LinkedList<MulticastMessage>> acks;

    public AckChecker(HashMap<String, LinkedList<MulticastMessage>> acks,
                      MulticastMessage message, ArrayList<String> memberList) {
        this.acks = MessagePasser.getInstance().getAcks();
        this.message = message;
        this.memberList = memberList;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(1000);
            
            String key = message.getSrcGroup() + message.getSrc() + message.getNum();
            if(acks.get(key).size() == (memberList.size() - 1)) {
                System.out.println("Got enough Ack for message: " + key);
                return;
            }
            else {
                Thread.sleep(4000);
                
                if(acks.get(key).size() == memberList.size() - 1) {
                    System.out.println("Got enough Ack for message: " + key);
                    return;
                }
                else {
                    System.out.println("Timeout for message: " + key + "Resend.");
                    MessagePasser.getInstance().send(message);
                }
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
