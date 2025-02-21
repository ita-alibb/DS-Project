package it.distributedsystems.connection;

import it.distributedsystems.messages.*;
import it.distributedsystems.messages.queue.*;
import it.distributedsystems.tui.TUIUpdater;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * This class handles the connection with the Broker net (the Leader)
 */
public class ClientConnection implements Runnable{
    private static ClientConnection INSTANCE;

    /**
     * The clientID, it's -1 at the beginning, then it is updated with the one sent from the leader (only one change per lifetime)
     */
    private static int clientID = -1;

    /**
     * List keeping formatted {IP}:{Port} of other brokers (to use if current leader fails to connect)
     */
    private static List<String> otherBrokers = new ArrayList<>();

    private String serverIP;
    private int serverPort;

    /**
     * The client socket
     */
    private Socket socket;

    /**
     * The socket out stream
     */
    private PrintWriter out;

    /**
     * The socket in stream
     */
    private BufferedReader in;

    private final ExecutorService processResponseThread = Executors.newSingleThreadExecutor();

    /**
     * This list keeps tracks of sent commandIds, removed when ack is received
     */
    private final List<QueueCommand> sentCommands = Collections.synchronizedList(new ArrayList<>());


    private final LinkedBlockingQueue<QueueResponse> asynchronousResponseQueue = new LinkedBlockingQueue<>();

    private ClientConnection(String serverIp, String tcpPort) {
        this.serverIP = serverIp;
        this.serverPort = Integer.parseInt(tcpPort);
    }

    public static void setConnection(String serverIp, String tcpPort) {
        if (INSTANCE == null) {
            INSTANCE = new ClientConnection(serverIp, tcpPort);
        }
    }

    public static ClientConnection getINSTANCE() {
        return INSTANCE;
    }

    /**
     * Runs the listening thread of the socket.
     */
    @Override
    public void run() {
        // Initialize Listening thread
        System.out.println("Connection Thread started");

        // Start thread to process responses (not stop the listening on the socket)
        this.processResponseThread.execute(this::processResponseAsync);


        while (true) {
            // Initialize the connection. Contains endless thread
            this.initConnection();

            TUIUpdater.getINSTANCE().reprintViewAsync(true);

            System.out.println("Connection established correctly, start listening");
            // Listening to socket. Contains endless while, returns only in case of exception eg: disconnection of leader
            this.listeningConnection();
        }
    }

    private void initConnection() {
        ConnectionResponse connectionResponse;
        int i=0;
        boolean retry = false;
        do {
            try {

                System.out.printf("Try connection with Broker %s and port %d \n", this.serverIP, this.serverPort);
                // establish connection to server
                this.socket = new Socket(this.serverIP, this.serverPort);

                this.out = new PrintWriter(this.socket.getOutputStream(), true);
                this.in = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));

                //Connection established
                //Send my id (if -1 it means I need the first id from the leader
                out.println(new ConnectionMessage(clientID).toJson());
                out.flush();//ensure sending

                connectionResponse = (ConnectionResponse) GsonDeserializer.deserialize(in.readLine());

                if (connectionResponse.isRedirect()) {
                    if (connectionResponse.getLeaderIP() != null && connectionResponse.getLeaderPort() != -1) {
                        this.serverIP = connectionResponse.getLeaderIP();
                        this.serverPort = connectionResponse.getLeaderPort();
                    } else {
                        System.out.println("The leader is not elected yet, retry after 30 seconds");
                        Thread.sleep(30_000);
                    }
                } else {
                    clientID = connectionResponse.getClientID();
                    otherBrokers = connectionResponse.getOtherBrokers();
                }

                retry = connectionResponse.isRedirect();
            } catch (Exception e) {//the current broker is down, try another one (should not happen at first start because we give the valid leader as argument of the program
                System.out.println("Exception on trying to connect to Leader : '"+ e.getMessage() + "' , try with another Broker");
                var address = otherBrokers.get(i).split(":");
                i = (i +1) % otherBrokers.size();
                this.serverIP = address[0];
                this.serverPort = Integer.parseInt(address[1]);
                retry = true;
            }
        } while (retry);
    }

    private void listeningConnection(){
        try {
            String jsonResponse;

            while((jsonResponse = in.readLine()) != null){
                /*System.out.println("received: " + jsonResponse);*/

                try {
                    QueueResponse res = (QueueResponse) GsonDeserializer.deserialize(jsonResponse); //I am sure that here only queue response command can arrive

                    //put the response in the queue, will be processed by the processResponseThread
                    this.asynchronousResponseQueue.put(res);
                } catch (Exception e) {
                    System.out.println("Deserialize throw exception:" + e.getMessage());
                }
            }
        } catch (IOException e) {
            // break the loop and finally call the disconnection
            System.out.println("Error while waiting connection of the Leader");
        } finally {
            try {
                this.socket.close();
            } catch (IOException e) {
                System.out.println("Exception closing socket: exception: " + e.getMessage());
            }
        }
    }

    public static int getClientId() {
        return clientID;
    }

    public static List<String> getOtherBrokers() {
        return otherBrokers;
    }

    private void processResponseAsync() {
        while (true) {
            try {
                var response = this.asynchronousResponseQueue.take();

                if (response.getData() != null) { //this means it is a response to ReadData: Show the data
                    //In Create a model that on update triggers tui Updater review
                    //TODO:change it to something more smart
                    TUIUpdater.getINSTANCE().printError("Last read: " + response.getData());
                }

                System.out.println("DEBUG: " + response.toJson() + " sentcommands: " + sentCommands.size());

                //remove the command id from the uncommited command list:
                sentCommands.removeIf(c -> c.getCommandID() == response.getCommandId());
                TUIUpdater.getINSTANCE().reprintViewAsync(true);
            } catch (InterruptedException e) {
                System.out.println("Error on executing broadcast");
            }
        }
    }
    /**
     * Synchronized method to be sure that the response is correct
     * Method used to send the command to the server
     *
     * @param command the command to send
     */
    public synchronized void sendAsync(QueueCommand command) {
        try {
            this.out.println(command.toJson());
            this.sentCommands.add(command);
            TUIUpdater.getINSTANCE().reprintViewAsync(true);
        } catch (Exception e) {
            System.out.println("Exception on sending request to server; exception: " + e.getMessage());
        }
    }

    public List<QueueCommand> getSentCommands() {
        return sentCommands;
    }
}

