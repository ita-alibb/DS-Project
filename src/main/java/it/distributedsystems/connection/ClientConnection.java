package it.distributedsystems.connection;

import it.distributedsystems.messages.*;
import it.distributedsystems.messages.queue.*;
import it.distributedsystems.utils.BrokerAddress;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class handles the connection with the Broker net (the Leader)
 */
public class ClientConnection implements Runnable{
    private static ClientConnection INSTANCE;
    private static final long WAIT_ELECTION_TIME = 30_000;

    /**
     * The clientID, it's -1 at the beginning, then it is updated with the one sent from the leader (only one change per lifetime)
     */
    private int clientID;

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

    private final ExecutorService threadPool = Executors.newFixedThreadPool(3);

    /**
     * This list keeps tracks of sent commandIds, removed when ack is received
     */
    private final List<QueueCommand> sentCommands = Collections.synchronizedList(new ArrayList<>());

    /**
     * List of commands to send
     */
    private final LinkedBlockingQueue<QueueCommand> commandsToSend = new LinkedBlockingQueue<>();

    private String lastError = "";
    private Integer lastReadInt = null;

    private final LinkedBlockingQueue<QueueResponse> asynchronousResponseQueue = new LinkedBlockingQueue<>();

    private final AtomicBoolean leaderAlive = new AtomicBoolean(false);
    private static final ScheduledExecutorService reRunnerScheduler = Executors.newSingleThreadScheduledExecutor();


    private ClientConnection(String serverIp, String tcpPort, int clientID) {
        this.serverIP = serverIp;
        this.serverPort = Integer.parseInt(tcpPort);
        this.clientID = clientID;

        // Start thread to process responses (not stop the listening on the socket)
        this.threadPool.execute(this::processResponseAsync);
    }

    public static void setConnection(String serverIp, String tcpPort, int clientID) {
        if (INSTANCE == null) {
            INSTANCE = new ClientConnection(serverIp, tcpPort, clientID);
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

        // Initialize the connection. Contains endless thread
        this.initConnection();
        this.leaderAlive.set(true);

        System.out.println("Connection established correctly, start listening");

        // Start listening thread
        this.threadPool.execute(this::listeningConnection);
        // Start sending thread
        this.threadPool.execute(this::commandSender);
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
                        Thread.sleep(WAIT_ELECTION_TIME);
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

            while((jsonResponse = this.in.readLine()) != null && leaderAlive.get()){
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
            if (leaderAlive.get()) {
                //Re-run, re-try connection after 5 seconds if leader crash
                reRunnerScheduler.schedule(getINSTANCE(), WAIT_ELECTION_TIME, TimeUnit.MILLISECONDS);
            }
            leaderAlive.set(false);
            try {
                this.socket.close();
            } catch (IOException e) {
                System.out.println("Exception closing socket: exception: " + e.getMessage());
                this.socket = null;
            }
        }
    }

    private void commandSender(){
        try {
            while (leaderAlive.get()) {
                var command = commandsToSend.take();

                this.out.println(command.toJson());
                this.sentCommands.add(command);
            }
        } catch (Exception e) {
            System.out.println("Exception on sending request to server; exception: " + e.getMessage());
        } finally {
            if (leaderAlive.get()) {
                //Re-run, re try connection after 5 seconds if leader crash
                reRunnerScheduler.schedule(getINSTANCE(), WAIT_ELECTION_TIME, TimeUnit.MILLISECONDS);
            }
            leaderAlive.set(false);
            try {
                this.socket.close();
            } catch (IOException e) {
                System.out.println("Exception closing socket: exception: " + e.getMessage());
                this.socket = null;
            }
        }
    }

    public int getClientId() {
        return this.clientID;
    }

    public static List<String> getOtherBrokers() {
        return otherBrokers;
    }

    private void processResponseAsync() {
        while (true) {
            try {
                var response = this.asynchronousResponseQueue.take();

                if (response.getData() != null) { //this means it is a response to ReadData: Show the data
                    lastReadInt = response.getData();
                }

                lastError = response.getError() == null ?
                        lastError : "CommandID:" + response.getCommandId() + " returned error: " + response.getError();

                //remove the command id from the uncommited command list:
                sentCommands.removeIf(c -> c.getCommandID() == response.getCommandId());
            } catch (InterruptedException e) {
                System.out.println("Error on executing broadcast");
            }
        }
    }
    /**
     * Add the command to send to the queue
     */
    public synchronized void sendAsync(QueueCommand command) {
        try{
            this.commandsToSend.put(command);
        } catch (InterruptedException e) {
            System.out.println("Exception while putting on queue");
        }
    }

    public List<QueueCommand> getSentCommands() {
        return sentCommands;
    }

    public String getLastError() {
        return lastError;
    }

    public Integer getLastReadInt() {
        return lastReadInt;
    }

    public BrokerAddress getBrokerAddress() {
        var ba = new BrokerAddress();
        ba.IP = this.serverIP;
        ba.ClientServerPort = this.serverPort;
        return ba;
    }
}

