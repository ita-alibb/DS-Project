package it.distributedsystems.connection;

import com.sun.jdi.ClassNotPreparedException;
import it.distributedsystems.connection.handler.ClientHandler;
import it.distributedsystems.connection.handler.LeaderHandler;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.queue.QueueResponse;
import it.distributedsystems.messages.raft.AppendEntries;
import it.distributedsystems.messages.raft.PastClientInfos;
import it.distributedsystems.raft.*;
import it.distributedsystems.raft.processors.ClientCommandProcessor;
import it.distributedsystems.raft.processors.ElectionProcessor;
import it.distributedsystems.raft.processors.RaftCommandProcessor;
import it.distributedsystems.tui.TUIUpdater;
import it.distributedsystems.utils.BrokerAddress;
import it.distributedsystems.utils.ElectionTimer;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

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
     */
    private ServerSocket brokerServerSocket;

    /**
     * Thread pool for accept endless thread of clients and brokers
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
     * List of all disconnected clients, kept by the Leader.
     * Initialized by looking at the whole log.
     */
    private ConcurrentHashMap<Integer, PastClientInfos> pastClientInfos;

    public PastClientInfos getPastClientInfos(int pastClientId) {
        return pastClientInfos.get(pastClientId);
    }


    /**
     * List of all connected Followers, used if leader
     */
    private final List<Follower> followerHandlers = new ArrayList<>();

    /**
     * Leader listening thread pool
     */
    private final ExecutorService leaderPool = Executors.newSingleThreadExecutor();
    /**
     * Class that keeps track of the leader connection. Is a SocketHandler.
     * Null if not connected to the leader or this node is the leader.
     */
    private LeaderHandler leaderHandler = null;

    private ClientCommandProcessor clientCommandProcessor;

    public String getLastQueueCommand() {
        return clientCommandProcessor == null ? "" : clientCommandProcessor.getLastClientCommand();
    }

    private RaftCommandProcessor raftCommandsProcessor;

    private ElectionProcessor electionProcessor;

    /**
     * Thread pool for process messages endless thread of clients and brokers
     */
    private final ExecutorService processPool = Executors.newFixedThreadPool(2);

    /**
     * Election timeout thread
     */
    private final ElectionTimer timer;

    public long getWaitTimeForCurrentTimer(){
        return timer.getWaitTimeForCurrentTimer();
    }

    public Set<Integer> getAcceptedCount() {
        return electionProcessor.getAcceptedCount();
    }

    public Set<Integer> getDeniedCount() {
        return electionProcessor.getDeniedCount();
    }

    private BrokerConnection() {
        try{
            var tcpPort = BrokerSettings.getCtoBPort();
            this.clientServerSocket = new ServerSocket(tcpPort);
            System.out.println("Server Socket for CLIENTS started on port " + tcpPort);

            tcpPort = BrokerSettings.getBtoBPort();
            this.brokerServerSocket = new ServerSocket(tcpPort);
            System.out.println("Server Socket for BROKERS started on port " + (tcpPort));

            //Start the default RAFT Command processor that is in common for all Brokers
            raftCommandsProcessor = new RaftCommandProcessor();
            processPool.execute(raftCommandsProcessor);

            //populate the list of known brokers
            for (BrokerAddress ba : BrokerSettings.getBrokers(true)) {
                var newFollower = new Follower(ba, raftCommandsProcessor::handleRaftMessageCallback);
                this.followerHandlers.add(newFollower);
            }

            //Start the election thread in common to all brokers
            electionProcessor = new ElectionProcessor(this.followerHandlers);
            timer = new ElectionTimer(electionProcessor::startElection);
            resetElectionTimeout();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException("Error on getting instantiate conneciton");
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
     * This function resets the timeout
     */
    public synchronized void resetElectionTimeout() {
        timer.resetElectionTimer();
    }

    /**
     * Call this method if this node becomes the leader
     */
    public void setLeader(){
        //Stop Election
        BrokerSettings.setBrokerStatus(BrokerStatus.Leader);
        resetElectionTimeout();

        clientCommandProcessor = new ClientCommandProcessor();
        processPool.execute(clientCommandProcessor);

        //cold start
        pastClientInfos = new ConcurrentHashMap<>(ReplicationLog.getPastClientsInfos());

        BrokerSettings.setLeaderAddress(BrokerSettings.getBrokerID());

        //Start the connection to every other broker (make them follower and identify as a LEADER)
        followerHandlers.forEach(Follower::connectHandler);
    }

    /**
     * Call this method to revert this node from Leader to Follower
     */
    public void setFollower() {
        BrokerSettings.setBrokerStatus(BrokerStatus.Follower);
        ReplicationLog.cleanCache();
        if (clientCommandProcessor != null) clientCommandProcessor.destroy();
    }

    /**
     * Function to retrieve the next unique client ID
     */
    public int getNewClientId() {
        var currentMax = Math.max(
                pastClientInfos.keySet().stream().mapToInt(x -> x).max().orElse(-1),
                clientHandlers.stream().mapToInt(ClientHandler::getClientId).max().orElse(-1)
        );

        return currentMax + 1;
    }

    /**
     * Function run on an endless loop thread.
     * Keeps accepting clients (if leader) or redirect to leader (if follower)
     */
    private void clientAccept() {
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
     * Called on client disconnection to enable it for reconnection
     */
    public void disconnectClientHandler(PastClientInfos newPastClientInfos){
        //Add the past client infos
        pastClientInfos.put(newPastClientInfos.getClientId(), newPastClientInfos);

        //Delete from current handlers
        clientHandlers.removeIf(ch -> ch.getClientId() == newPastClientInfos.getClientId());
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
        clientHandler.setLastResponse(response);
        clientHandler.sendMessage(response);
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
                TUIUpdater.setLastMessage("New broker connected: " + brokerSocket.getInetAddress());

                //Establishing connection
                try{
                    //Create handler that initialize the connection
                    handler = new LeaderHandler(brokerSocket);

                    setFollower();
                    resetElectionTimeout();

                    //Reach here if the constructor does not throw exception (The leader is contacting you)
                    handler.setMsgReceiveCallback(raftCommandsProcessor::handleRaftMessageCallback);

                    //kill previous leader
                    if (leaderHandler != null) {
                        leaderHandler.shutDownLeaderHandler();
                    }

                    leaderHandler = handler; //track new leader
                    leaderPool.execute(handler);//execute new leader

                    BrokerSettings.setLeaderAddress(handler.getLeaderId());//set the leader address
                    TUIUpdater.setLastMessage("New LEADER created and added correctly ID: " + handler.getLeaderId());
                } catch (IOException e) {
                    TUIUpdater.setLastMessage("Error while establishing client connection: " + e.getMessage());
                } catch (ClassNotPreparedException e) {
                    TUIUpdater.setLastMessage("Answered RequestVote");
                }
            } catch (IOException e) {
                TUIUpdater.setLastMessage("Error while waiting for broker connection");
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
     * Send Message to every follower
     */
    public void forwardAllFollowers(AppendEntries message) {
        //Start it in a new thread to not stop the process execution
        if (this.followerHandlers.isEmpty()) return;

        ExecutorService exec = Executors.newFixedThreadPool(this.followerHandlers.size());
        try {
            for (final Follower fh : this.followerHandlers) {
                exec.submit(() -> fh.sendAppendEntries(message));
            }
        } finally {
            exec.shutdown();
        }
    }

    public void decreaseFollowerNextIndex(int followerId) {
        followerHandlers.stream().filter(f -> f.getFollowerId() == followerId).findFirst()
                .ifPresent(Follower::decreaseNextIndex);
    }

    public void increaseFollowerIndexes(int followerId, int lastReceivedIndex) {
        followerHandlers.stream().filter(f -> f.getFollowerId() == followerId).findFirst()
                .ifPresent(fw ->fw.increaseIndexes(lastReceivedIndex));
    }

    public List<Follower> getFollowers() {
        return new ArrayList<>(followerHandlers);
    }

    public void registerResponse(QueueResponse response) {
        if (clientCommandProcessor == null || BrokerSettings.getBrokerStatus() != BrokerStatus.Leader) return; //not the leader, nothing to forward
        clientCommandProcessor.handleResponseQueueCallback(response);
    }
}
