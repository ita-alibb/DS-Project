package it.distributedsystems.connection;

import com.sun.jdi.ClassNotPreparedException;
import it.distributedsystems.connection.handler.ClientHandler;
import it.distributedsystems.connection.handler.FollowerHandler;
import it.distributedsystems.connection.handler.LeaderHandler;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.queue.QueueResponse;
import it.distributedsystems.raft.*;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.System.exit;

public class BrokerConnection {
    private static BrokerConnection INSTANCE;

    /**
     * The server socket to which the clients write (to be used if the broker is the leader)
     */
    private ServerSocket clientServerSocket;

    /**
     * The server socket that waits on accept.
     * It holds one stable connection (the one with the leader to receive the AppendEntries)
     * and always in accept because some follower may start an election.
     * "rule" set it as clientServerSocket+1
     */
    private ServerSocket brokerServerSocket;

    /**
     * Thread pool for accept endless thread of clients and borkers
     */
    private final ExecutorService acceptancePool = Executors.newFixedThreadPool(2);

    /**
     * clients handling thread pool Max 10 clients
     */
    private final ExecutorService clientsPool = Executors.newFixedThreadPool(10);
    /**
     * List of all connected Clients
     */
    private final List<ClientHandler> clientHandlers = new ArrayList<>();

    /**
     * Brokers listening thread pool Max 10 followers
     */
    private final ExecutorService followersPool = Executors.newFixedThreadPool(10);
    /**
     * List of all connected Followers, used if leader
     */
    private final List<FollowerHandler> followerHandlers = new ArrayList<>();

    /**
     * Leader listening thread pool
     */
    private final ExecutorService leaderPool = Executors.newSingleThreadExecutor();
    /**
     * Class that keeps track of the leader connection. Is a ScoketHandler.
     * Null if not connected to the leader or this node is the leader.
     */
    private LeaderHandler leaderHandler;

    /**
     * Centralized queue that receives every message from the Brokers
     */
    private final BlockingQueue<BaseDeserializableMessage> raftCommandsQueue = new LinkedBlockingQueue<>();

    private ClientCommandProcessor clientCommandProcessor;

    private RaftCommandProcessor raftCommandsProcessor;

    /**
     * Thread pool for process messages endless thread of clients and borkers
     */
    private final ExecutorService processPool = Executors.newFixedThreadPool(2);

    private BrokerConnection() {
        try{
            var tcpPort = BrokerSettings.getCtoBPort();
            this.clientServerSocket = new ServerSocket(tcpPort);
            System.out.println("Server Socket for CLIENTS started on port " + tcpPort);

            tcpPort = BrokerSettings.getBtoBPort();
            this.brokerServerSocket = new ServerSocket(tcpPort+1);
            System.out.println("Server Socket for BROKERS started on port " + (tcpPort));

            //Start the default RAFT Command processor that is in common for all Brokers
            raftCommandsProcessor = new RaftCommandProcessor();
            processPool.execute(raftCommandsProcessor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static synchronized BrokerConnection getInstance(){
        if (INSTANCE == null){
            INSTANCE = new BrokerConnection();
        }
        return INSTANCE;
    }

    /**
     * Starts all the thread
     */
    public static void start(){
        getInstance().startConnection();
    }

    private void startConnection(){
        //Start the accepting thread on clientServerSocket and brokerServerSocket
        acceptancePool.execute(this::clientAccept);
        acceptancePool.execute(this::brokerAccept);
    }

    /**
     * Call this method if this node becomes the leader
     */
    public void setLeader(){
        clientCommandProcessor = new ClientCommandProcessor();
        processPool.execute(clientCommandProcessor);
        BrokerSettings.setBrokerStatus(BrokerStatus.Leader);
    }

    /**
     * Function run on an endless loop thread.
     * Keeps accepting clients (if leader) or redirect to leader (if follower)
     */
    private void clientAccept() {
        // TODO: REMOVE THIS IS JUST TO TEST
        this.setLeader();

        while (true) {
            try{
                Socket clientSocket = this.clientServerSocket.accept();
                ClientHandler handler;
                System.out.println("New client connected: " + clientSocket.getInetAddress() + clientSocket.getPort());

                try{
                    //Create handler that initialize the connection
                    handler = new ClientHandler(clientSocket);

                    //Reach here if the constructor does not throw exception (You ARE the leader)
                    handler.setMsgReceiveCallback(clientCommandProcessor::handleClientMessageCallback);
                    clientHandlers.add(handler);
                    clientsPool.execute(handler);

                    System.out.println("New handler created and added correctly ID: " + handler.getClientId());
                } catch (IOException e) {
                    System.out.println("Error while establishing client connection: " + e.getMessage());
                } catch (ClassNotPreparedException e) {
                    System.out.println("Not leader, redirect to leader");
                }
            } catch (IOException e) {
                System.out.println("Error while waiting for client connection");
                exit(-1);
            }
        }
    }

    /**
     * Send QueueResponse to specific client Id
     */
    public void sendQueueResponseToClient(QueueResponse response) {
        var clientHandler = this.clientHandlers.stream().filter(ch -> ch.getClientId() == response.getClientID()).findFirst().orElse(null);
        if (clientHandler == null) {
            System.out.println("Client " + response.getClientID() + " not found, cannot send response: " + response.toJson());
            return;
        }

        clientHandler.sendMessage(response);
    }

    /**
     * Called by Leader at startup. Initializes the handlers.
     * When a Follower crashes Handler must be removed!.
     * When a Follower recovers from crash he will wait for x time for the AppendEntries. If not received he will start an election, this way he will receive the new leader!
     */
    private void connectToEveryNode(){
        var allNodesAddress = BrokerSettings.getBrokers();
        if (followerHandlers.size() == allNodesAddress.size()) return; //Every node is connected

        List<Integer> presentFollowerIDs = followerHandlers.stream().map(FollowerHandler::getFollowerId).toList();

        for (Integer followerID : presentFollowerIDs) {
            connectToNode(followerID);
        }
    }

    /**
     * Connect the current broker Leader to the follower
     */
    private void connectToNode(Integer followerID) {
        var followerAddress = BrokerSettings.getBrokers().stream().filter(a -> a.id == followerID).findFirst().orElse(null);
        if (followerAddress == null) {
            System.out.println("No follower with given id: " + followerID);
            return;
        }

        try {
            var followerSocket = new Socket(followerAddress.IP, followerAddress.BrokerServerPort);

            try (PrintWriter out = new PrintWriter(followerSocket.getOutputStream(), true)){
                //Send the connection message to identify as a leader
                // TODO: out.println(new LeaderConnection(BrokerSettings.getBrokerID(),BrokerSettings.getBrokerStatus()));

                // TODO: FollowerHandler handler = new FollowerHandler(followerID,leaderLastIndex, followerSocket, out, new BufferedReader(new InputStreamReader(followerSocket.getInputStream())), clientCommandsProcesser::handleClientMessageCallback);

                // TODO: followerHandlers.add(handler);
                // TODO: followersPool.execute(handler);
            } catch (IOException e) {
                System.out.println("Error while establishing client connection: " + e.getMessage());
            }
        } catch (Exception e) {
            System.out.println("Follower is not ready");
        }
    }

    /**
     * Function run on an endless loop thread.
     * Keeps accepting brokers.
     * If the connection is from the leader, stores the persistent connection.
     * If is from another follower it means it is an election starting
     */
    private void brokerAccept() {
        while (true) {
            try {
                Socket brokerSocket = this.brokerServerSocket.accept();
                LeaderHandler handler;
                System.out.println("New broker connected: " + brokerSocket.getInetAddress());

                //Establishing connection
                try{
                    //Create handler that initialize the connection
                    handler = new LeaderHandler(brokerSocket);

                    //Reach here if the constructor does not throw exception (The leader is contacting you)
                    handler.setMsgReceiveCallback(raftCommandsProcessor::handleRaftMessageCallback);

                    //kill previous leader
                    if (leaderHandler != null) {
                        leaderHandler.shutDownLeaderHandler();
                    }
                    leaderPool.shutdownNow();

                    leaderHandler = handler; //track new leader
                    leaderPool.execute(handler);//execute new leader

                    System.out.println("New LEADER created and added correctly ID: " + handler.getLeaderId());
                } catch (IOException e) {
                    System.out.println("Error while establishing client connection: " + e.getMessage());
                } catch (ClassNotPreparedException e) {
                    System.out.println("Not leader, redirect to leader");
                }
            } catch (IOException e) {
                System.out.println("Error while waiting for broker connection");
            }
        }
    }

    /**
     * Send message to Leader
     */
    public void sendMessageToLeader(BaseDeserializableMessage response) {
        if (leaderHandler == null) {
            System.out.println("Leader is not connected, did not send message: " + response.toJson());
            return;
        }

        leaderHandler.sendMessage(response);
    }
    /**
     * Call back function called by every LeaderHandler upon receiving of a message
     */
    public void handleLeaderMessageCallback(String jsonMessage) throws InterruptedException {

    }

    /**
     * Call back function called by every FollowerHandler upon receiving of a message
     */
    public void handleBrokerMessageCallback(String jsonMessage) throws InterruptedException {
        BaseDeserializableMessage cmd = GsonDeserializer.deserialize(jsonMessage);
        // Add the message to the shared queue of brokers
        raftCommandsQueue.put(cmd);
    }

    /**
     * Send Message to every follower
     */
    public void forwardAllFollowers(BaseDeserializableMessage message) {
        //Start it in a new thread to not stop the process execution
        new Thread(() -> {
            this.followerHandlers.forEach(fh -> {
                fh.sendMessage(message);
            });
        });
    }
}
