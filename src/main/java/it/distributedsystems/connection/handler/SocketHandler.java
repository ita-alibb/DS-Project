package it.distributedsystems.connection.handler;

import it.distributedsystems.connection.ReceiveJsonMessageCallback;
import it.distributedsystems.messages.BaseDeserializableMessage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

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
    protected ReceiveJsonMessageCallback msgReceiveCallback;

    /**
     * Atomic boolean to be sure to killed the socketHandler
     */
    private final AtomicBoolean killed;

    public SocketHandler(Socket socket) throws IOException {
        this.socket = socket;
        this.out = new PrintWriter(socket.getOutputStream(), true);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.msgReceiveCallback = null;
        killed = new AtomicBoolean(false);
    }

    public SocketHandler(Socket socket, PrintWriter out, BufferedReader in) throws IOException {
        this.socket = socket;
        this.out = out;
        this.in = in;
        this.msgReceiveCallback = null;
        killed = new AtomicBoolean(false);
    }

    public SocketHandler(Socket socket, ReceiveJsonMessageCallback msgReceiveCallback) throws IOException {
        this.socket = socket;
        this.out = new PrintWriter(socket.getOutputStream(), true);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.msgReceiveCallback = msgReceiveCallback;
        killed = new AtomicBoolean(false);
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
            try {
                this.socket.close();
            } catch (IOException e) {
                System.out.println("Exception closing socket: exception: " + e.getMessage());
            }
            killed.set(true);
            customDisconnection();
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
            killed.set(true);
            customDisconnection();
        }
    }

    /**
     * Used to set the callback
     */
    public void setMsgReceiveCallback(ReceiveJsonMessageCallback msgReceiveCallback) {
        if (this.msgReceiveCallback != null) {
            System.out.println("Cannot re set CallBack");
            return;
        }

        this.msgReceiveCallback = msgReceiveCallback;
    }

    public boolean isConnected(){
        return (this.socket != null && !this.socket.isClosed() && !killed.get());
    }

    /**
     * To override by other class for custom behavior.
     * WARNING: may be called twice
     */
    protected void customDisconnection(){
        //None
    }

    public String getInetAddress(){
        if (isConnected()) {
            return socket.getInetAddress().toString();
        }

        return "";
    }
}
