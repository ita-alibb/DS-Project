package it.distributedsystems.connection;

import it.distributedsystems.connection.handler.ClientHandler;
import it.distributedsystems.connection.handler.FollowerHandler;
import it.distributedsystems.connection.handler.LeaderHandler;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.queue.ConnectionMessage;
import it.distributedsystems.messages.queue.ConnectionResponse;
import it.distributedsystems.messages.queue.QueueCommand;
import it.distributedsystems.messages.queue.QueueResponse;
import it.distributedsystems.messages.raft.AppendEntries;
import it.distributedsystems.raft.*;
import it.distributedsystems.utils.BrokerAddress;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class BrokerConnection {
    private static BrokerConnection INSTANCE;

    /**
     * The next client ID that will be assigned to the next clients that will connect
     */
    private int nextClientId = 0;

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
     * Thread pool for process messages endless thread of clients and borkers
     */
    private final ExecutorService processPool = Executors.newFixedThreadPool(2);

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
     * Class that keeps track of the leader connection. Is a ScoketHandler.
     * Null if not connected to the leader or this node is the leader.
     */
    private LeaderHandler leaderHandler;

    /**
     * Centralized queue that receives every message from the (possibly) different clients
     */
    private final BlockingQueue<QueueCommand> commandsQueue = new LinkedBlockingQueue<>();

    /**
     * Centralized queue that receives every message from the Brokers
     */
    private final BlockingQueue<BaseDeserializableMessage> raftCommandsQueue = new LinkedBlockingQueue<>();

    private CommandProcessor clientCommandsProcesser;

    private BrokerConnection() {
        try{
            var tcpPort = BrokerSettings.getCtoBPort();
            this.clientServerSocket = new ServerSocket(tcpPort);
            System.out.println("Server Socket for CLIENTS started on port " + tcpPort);

            tcpPort = BrokerSettings.getBtoBPort();
            this.brokerServerSocket = new ServerSocket(tcpPort+1);
            System.out.println("Server Socket for BROKERS started on port " + (tcpPort));
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
        acceptancePool.submit(this::clientAccept);
        acceptancePool.submit(this::brokerAccept);

        // Start a separate thread to process messages from brokers
        processPool.submit(this::processBrokerCommand);
    }

    /**
     * Call this method if this node becomes the leader
     */
    public void setLeader(){
        clientCommandsProcesser = new CommandProcessor();
        processPool.submit(clientCommandsProcesser);
        BrokerSettings.setBrokerStatus(BrokerStatus.Leader);
    }

    /**
     * Function run on an endless loop thread.
     * Keeps accepting clients (if leader) or redirect to leader (if follower)
     */
    public void clientAccept() {
        while (true) {
            try{
                Socket clientSocket = this.clientServerSocket.accept();
                ClientHandler handler;
                System.out.println("New client connected: " + clientSocket.getInetAddress());

                //Establishing connection
                try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
                    try (PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)){
                        ConnectionMessage msg = (ConnectionMessage) GsonDeserializer.deserialize(in.readLine()); //receive the connection message

                        if (BrokerSettings.getBrokerStatus() != BrokerStatus.Leader) {
                            //I am NOT the leader, so I can't connect with the client.
                            //Send back the IP/Port of the leader
                            var leaderAddress = BrokerSettings.getLeaderAddress();
                            out.println(new ConnectionResponse(leaderAddress.IP,leaderAddress.ClientServerPort).toJson());
                            //Close the socket
                            continue; //do not submit in the clientsPool
                        }

                        //Handle the assign of an ID.
                        //Send back the client ID. (nextClientId++)
                        var newClientId = msg.getClientID() == -1 ? nextClientId++ : msg.getClientID();
                        out.println(new ConnectionResponse(newClientId, BrokerSettings.getBrokers().stream().map(BrokerAddress::addressStringForClient).toList()).toJson());

                        //Create the ClientHandler
                        handler = new ClientHandler(newClientId, clientSocket, out, in, clientCommandsProcesser::handleClientMessageCallback);

                        clientHandlers.add(handler);
                        clientsPool.submit(handler);
                    }
                } catch (IOException e) {
                    System.out.println("Error while establishing client connection: " + e.getMessage());
                    continue;
                }
            } catch (IOException e) {
                System.out.println("Error while waiting for client connection");
            }
        }
    }

    /**
     * Send QueueResponse to specific client Id
     */
    public void sendQueueResponseToClient(int clientID, QueueResponse response) {
        var clientHandler = this.clientHandlers.stream().filter(ch -> ch.getClientId() == clientID).findFirst().orElse(null);
        if (clientHandler == null) {
            System.out.println("Client " + clientID + " not found, cannot send response: " + response.toJson());
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
                out.println(new LeaderConnection(BrokerSettings.getBrokerID(),BrokerSettings.getBrokerStatus()));

                FollowerHandler handler = new FollowerHandler(followerID,leaderLastIndex, followerSocket, out, new BufferedReader(new InputStreamReader(followerSocket.getInputStream())), clientCommandsProcesser::handleClientMessageCallback);

                followerHandlers.add(handler);
                followersPool.submit(handler);
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
    public void brokerAccept() {
        while (true) {
            try {
                Socket brokerSocket = this.brokerServerSocket.accept();
                System.out.println("New broker connected: " + brokerSocket.getInetAddress());

                //Establishing connection
                try (BufferedReader in = new BufferedReader(new InputStreamReader(brokerSocket.getInputStream()))) {
                    try (PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true)){
                        BaseDeserializableMessage msg = GsonDeserializer.deserialize(in.readLine()); //receive the connection message

                        if (msg instanceof RequestVote) {//Message from another follower (candidate) to RequestVote RPC
                            // if conditions to vote, vote Yes else vote no
                        } else if (msg instanceof LeaderConnection) {//Message from the leader, it connect to this follower.
                            //create LeaderHandler to keep persistent connection
                        }
                    }
                } catch (IOException e) {
                    System.out.println("Error while establishing client connection: " + e.getMessage());
                    continue;
                }
            } catch (IOException e) {
                System.out.println("Error while waiting for broker connection");
            }
        }
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
