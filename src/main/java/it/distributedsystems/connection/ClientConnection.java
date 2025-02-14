package it.distributedsystems.connection;

import it.distributedsystems.messages.*;
import it.distributedsystems.messages.queue.*;

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
    /**
     * The clientID, it's -1 at the beginning, then it is updated with the one sent from the leader (only one change per lifetime)
     */
    private static int clientID = -1;

    /**
     * List keeping formatted {IP}:{Port} of other brokers (to use if current leader fails to connect)
     */
    private static List<String> otherBrokers;

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
    private final List<Integer> sentCommandIds = Collections.synchronizedList(new ArrayList<>());


    private final LinkedBlockingQueue<QueueResponse> asynchronousResponseQueue = new LinkedBlockingQueue<>();

    public ClientConnection(String serverIp, String tcpPort) {
        initConnection(serverIp,Integer.parseInt(tcpPort));
    }

    private void initConnection(String serverIp, int tcpPort) {
        ConnectionResponse connectionResponse;
        int i=1;
        boolean retry = false;
        do {
            try {

                System.out.printf("Try connection with Broker %s and port %d \n", serverIp, tcpPort);
                // establish connection to server
                this.socket = new Socket(serverIp, tcpPort);

                this.out = new PrintWriter(this.socket.getOutputStream(), true);
                this.in = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));

                //Connection established
                //Send my id (if -1 it means I need the first id from the leader
                out.println(new ConnectionMessage(clientID).toJson());
                out.flush();//ensure sending

                connectionResponse = (ConnectionResponse) GsonDeserializer.deserialize(in.readLine());

                if (connectionResponse.isRedirect()) {
                    serverIp = connectionResponse.getLeaderIP();
                    tcpPort = connectionResponse.getLeaderPort();
                } else {
                    clientID = connectionResponse.getClientID();
                    otherBrokers = connectionResponse.getOtherBrokers();
                }

                retry = connectionResponse.isRedirect();
            } catch (Exception e) {//the current broker is down, try another one (should not happen at first start because we give the valid leader as argument of the program
                System.out.println("Exception on trying to connect to Leader, try with another Broker");
                var address = otherBrokers.get(1).split(":");
                serverIp = address[0];
                tcpPort = Integer.parseInt(address[1]);
                retry = true;
            }
        } while (retry);
    }

    public static int getClientId() {
        return clientID;
    }

    public static List<String> getOtherBrokers() {
        return otherBrokers;
    }

    /**
     * Runs the listening thread of the socket.
     */
    @Override
    public void run() {
        // Initialize Listening thread
        System.out.println("Listening Thread started");

        // Start thread to process responses (not stop the listening on the socket)
        this.processResponseThread.execute(this::processResponseAsync);

        String jsonResponse;

        try {
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
        } finally {
            try {
                this.socket.close();
            } catch (IOException e) {
                System.out.println("Exception closing socket: exception: " + e.getMessage());
            }
        }
    }

    private void processResponseAsync() {
        while (true) {
            try {
                var response = this.asynchronousResponseQueue.take();

                if (response.getData() != null) { //this means it is a response to ReadData: Show the data
                    //TODO: get the data and process it (show in the tui something like LastReadData: x,
                    // and you here update that value that is the one shown in the tui)
                }

                //remove the command id from the uncommited command list:
                sentCommandIds.remove(Integer.valueOf(response.getCommandId()));
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
    private synchronized void sendAsync(QueueCommand command) {
        try {
            this.out.println(command.toJson());
            this.sentCommandIds.add(command.getCommandID());
        } catch (Exception e) {
            System.out.println("Exception on sending request to server; exception: " + e.getMessage());
        }
    }
}

