package distSysLab2.logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class LoggerListenerThread implements Runnable {
    private int port;
    private ServerSocket listenSocket;
    private Thread thread;

    public LoggerListenerThread(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try {
            listenSocket = new ServerSocket(this.port);
            while(true) {
                // Listening for new incoming connection.
                Socket socket = listenSocket.accept();

                // Create a new thread for new incoming connection.
                thread = new Thread(new LoggerReceiverThread(socket));
                thread.start();
            }
        }
        catch (IOException e) {
            System.err.println("ERROR: server Socket error");
        }
    }

    public void teminate() throws IOException {
        thread.interrupt();
        listenSocket.close();
    }

    @Override
    public String toString() {
        return "Listener [port=" + port + "]";
    }
}
