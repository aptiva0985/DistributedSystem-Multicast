package distSysLab2.comm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import distSysLab2.message.TimeStampMessage;

public class UserInputThread implements Runnable {
    @Override
    public void run() {
        MessagePasser msgPasser = MessagePasser.getInstance();

        try {
            System.out.println("Enter command: (send/receive/status/time/exit)");
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            String command = "";
            while(!command.equals("quit")) {
                command = in.readLine();
                if(command.equals("send")) {
                    System.out.println("Message dest:");
                    String dest = in.readLine();
                    while(!msgPasser.getNodeList().containsKey(dest)) {
                        System.out.println("Dest not existed, try again:");
                        dest = in.readLine();
                    }

                    System.out.println("Message kind (any string):");
                    String kind = in.readLine();
                    System.out.println("Message data:");
                    String data = in.readLine();
                    System.out.println("Do you want it to be logged (true/false):");
                    Boolean willLog = (new Boolean(in.readLine())).booleanValue();

                    TimeStampMessage msg = new TimeStampMessage(dest, kind, data);
                    msgPasser.send(msg, willLog);

                    System.out.println("Send Success:");
                    System.out.println(msg.toString());
                }
                else if(command.equals("receive")) {
                    System.out.println("Do you want it to be logged (true/false):");
                    Boolean willLog = (new Boolean(in.readLine())).booleanValue();

                    TimeStampMessage msg = msgPasser.receive(willLog);
                    if(msg == null) {
                        System.out.println("Nothing received.");
                    }
                    else {
                        System.out.println("Received:");
                        System.out.println(msg.toString());
                    }
                }
                else if(command.equals("status")) {
                    System.out.println(msgPasser.toString());
                }
                else if(command.equals("time")) {
                    System.out.println(msgPasser.getClockServ().getCurTimeStamp());
                }
                else if(command.equals("exit")) {
                    //msgPasser.teminate();
                    System.out.println("Exit.");
                    System.exit(0);
                }
                else {
                    System.out.println("Invalid command. Try again.");
                }

                System.out.println("Enter command: (send/receive/status/time/exit)");
            }

            msgPasser.teminate();
            System.out.println("Exit.");
            System.exit(0);
        }
        catch (IOException e) {
            System.err.println("ERROR: Reader error");
            e.printStackTrace();
        }
    }

    public static String[] init() throws IOException {
        String[] result = new String[3];

        System.out.println("Enter the name of configuration file:");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        result[0] = in.readLine();

        System.out.println("Enter the name of your machine:");
        result[1] = in.readLine();

        System.out.println("Enter the name of logger:");
        System.out.println("(You may skip this step, but cannot use logger function anymore)");
        result[2] = in.readLine();

        return result;
    }

    public static void main(String[] args) throws IOException {
        try {
            String[] arg = init();
            MessagePasser messagePasser = MessagePasser.getInstance(arg[0], arg[1], arg[2]);
            messagePasser.startListener();
            messagePasser.startSender();

            UserInputThread uiThread = new UserInputThread();
            Thread thread = new Thread(uiThread);
            thread.start();
            //messagePasser.teminate();

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
