package distSysLab2.comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import distSysLab2.message.MulticastMessage;

public class AckChecker implements Runnable {
    
    private volatile HashMap<String, LinkedList<MulticastMessage>> acks;
    private MulticastMessage message;
    private ArrayList<String> memberList;

    public AckChecker(HashMap<String, LinkedList<MulticastMessage>> acks,
                      MulticastMessage message, ArrayList<String> memberList) {
        this.acks = acks;
        this.message = message;
        this.memberList = memberList;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(1000);
            
            String key = message.getSrcGroup() + message.getSrc() + message.getNum();
            System.out.println("444444 " + key);
            System.out.println(acks.get(key).size());
            if(acks.get(key).size() == memberList.size() - 1) {
                return;
            }
            else {
                Thread.sleep(4000);
                
                if(acks.get(key).size() == memberList.size() - 1) {
                    return;
                }
                else {
                    MessagePasser.getInstance().send(message, false);
                }
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
