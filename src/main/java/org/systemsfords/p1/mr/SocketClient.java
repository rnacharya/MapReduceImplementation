package org.systemsfords.p1.mr;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;

public class SocketClient {
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;
    ObjectOutputStream oos;
    public void startConnection(String ip, int port) throws Exception, IOException {
        clientSocket = new Socket(ip, port);
        //out = new PrintWriter(clientSocket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        oos = new ObjectOutputStream(clientSocket.getOutputStream());
    }
 
    public String sendMessage(String msg) throws Exception {
    	oos.writeObject(msg);
    	//out.println(msg);
        //String resp = in.readLine();
    	
        return null;
    }
 
    public void stopConnection() throws Exception {
        //in.close();
        //out.close();
        oos.close();
        clientSocket.close();
    }
}