package org.systemsfords.p1.mr;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class SocketClient {
    private Socket clientSocket;
    private ObjectOutputStream oos;
    
    public void startConnection(String ip, int port) throws Exception, IOException {
        clientSocket = new Socket(ip, port);
        oos = new ObjectOutputStream(clientSocket.getOutputStream());
    }
 
    public String sendMessage(String msg) throws Exception {
    	oos.writeObject(msg);
        return null;
    }
 
    public void stopConnection() throws Exception {
        oos.close();
        clientSocket.close();
    }
}