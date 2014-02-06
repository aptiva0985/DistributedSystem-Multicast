package distSysLab2.logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import distSysLab2.message.TimeStampMessage;

public class LoggerUIThread implements Runnable {
    ArrayList<TimeStampMessage> list = new ArrayList<TimeStampMessage>();

    @Override
    public void run() {
        try {
            System.out.println("Enter command: (print/flush/exit)");
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            String command = "";
            while(!command.equals("exit")) {
                command = in.readLine();

                if(command.equals("print")) {
                    list = (ArrayList<TimeStampMessage>) Logger.getInstance().showMessages();
                    
                    if(list.size() > 0) {
                        for(int i = 0; i < list.size() - 1; i++) {
                            System.out.println(list.get(i).getData());

                            if(list.get(i).compareTo(list.get(i + 1)) == 0) {
                                System.out.println("||");
                            }
                            else {
                                System.out.println("->");
                            }
                        }
                        
                        System.out.println(list.get(list.size() - 1).getData());
                    }
                    else {
                        System.out.println("No log.");
                    } 
                }
                else if(command.equals("flush")) {
                    Logger.getInstance().showMessages().clear();
                    System.out.println("Log cleared.");
                }
                else if(command.equals("exit")) {
                    break;
                }
                else {
                    System.out.println("Invalid command. Try again.");
                }
                
                System.out.println("Enter command: (print/flush/exit)");
            }

            System.out.println("Exit.");
            System.exit(0);
        }
        catch (IOException e) {
            System.err.println("ERROR: Reader error");
            e.printStackTrace();
        }
    }


    public static String[] init() throws IOException {
        String[] result = new String[2];

        System.out.println("Enter the name of configuration file:");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        result[0] = in.readLine();

        System.out.println("Enter the name of your machine(logger):");
        result[1] = in.readLine();

        return result;
    }

    public static void main(String[] args) throws IOException {
        String[] arg = init();
        Logger.getInstance(arg[0], arg[1]);
    }
}
