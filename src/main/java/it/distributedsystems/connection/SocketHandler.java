package it.distributedsystems.connection;

import it.distributedsystems.messages.BaseDeserializableMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * This class is used to handle the socket connected to my ServerSocket
 */
public class SocketHandler implements Runnable {
    /**
     * The connected socket
     */
    protected final Socket socket;

    /**
     * The socket's out stream
     */
    protected final PrintWriter out;

    /**
     * The socket's in stream
     */
    protected final BufferedReader in;

    /**
     * This is the function called after receiving a message
     */
    private final ReceiveJsonMessageCallback msgReceiveCallback;

    public SocketHandler(Socket socket, PrintWriter out, BufferedReader in, ReceiveJsonMessageCallback msgReceiveCallback){
        this.socket = socket;
        this.out = out;
        this.in = in;
        this.msgReceiveCallback = msgReceiveCallback;
    }

    /**
     * Runs the thread to listen to messages
     */
    @Override
    public void run() {
        String incomingMessage;
        try{
            while ((incomingMessage = this.in.readLine()) != null) {
                try{
                    msgReceiveCallback.onReceiveJsonMessage(incomingMessage);
                } catch (InterruptedException e){
                    System.out.println("Exception while waiting for queue to free space from socket IP:"+ socket.getInetAddress() + " Port: " + socket.getPort());
                }
            }
        } catch (IOException e) {
            // break the loop and finally call the disconnection
        } finally {
            System.out.println("Client disconnected from socket IP:"+ socket.getInetAddress() + " Port: " + socket.getPort());
            //TODO: better handling of disconnection?
        }
    }

    /**
     * Method to send the response through the socket out
     * @param message the message to send
     */
    public void sendMessage(BaseDeserializableMessage message) {
        try {
            this.out.println(message.toJson());
        } catch (Exception e) {
            System.out.println("Exception on sending message from socket IP:"+ socket.getInetAddress() + " Port: " + socket.getPort()  + " " + e.getMessage());
        }
    }
}
