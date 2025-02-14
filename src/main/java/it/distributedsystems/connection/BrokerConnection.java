package it.distributedsystems.connection;

import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.queue.BaseDeserializableMessage;
import it.distributedsystems.messages.queue.ConnectionMessage;
import it.distributedsystems.messages.queue.ConnectionResponse;
import it.distributedsystems.messages.queue.QueueCommand;
import it.distributedsystems.raft.BrokerModel;
import it.distributedsystems.raft.BrokerSettings;
import it.distributedsystems.raft.BrokerStatus;

import java.io.*;
import java.net.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class BrokerConnection {
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
     * clients listening thread pool Max 10 clients
     */
    ExecutorService clientsPool = Executors.newFixedThreadPool(10);

    /**
     * Brokers listening thread pool Max 10 clients
     */
    ExecutorService brokersPool = Executors.newFixedThreadPool(10);

    /**
     * Centralized queue that receives every message from the (possibly) different clients
     */
    private final BlockingQueue<QueueCommand> commandsQueue = new LinkedBlockingQueue<>();

    /**
     * Centralized queue that receives every message from the Brokers
     */
    private final BlockingQueue<BaseDeserializableMessage> raftCommandsQueue = new LinkedBlockingQueue<>();

    public BrokerConnection() {
        try{
            var tcpPort = BrokerSettings.getCtoBPort();
            this.clientServerSocket = new ServerSocket(tcpPort);
            System.out.println("Server Socket for CLIENTS started on port " + tcpPort);

            tcpPort = BrokerSettings.getBtoBPort();
            this.brokerServerSocket = new ServerSocket(tcpPort+1);
            System.out.println("Server Socket for BROKERS started on port " + (tcpPort));

            //Start the accepting thread on clientServerSocket and brokerServerSocket
            acceptancePool.submit(this::clientAccept);
            acceptancePool.submit(this::brokerAccept);

            // Start a separate thread to process messages from all clients and brokers
            processPool.submit(this::processClientsCommand);
            processPool.submit(this::processBrokerCommand);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Function run on an endless loop thread.
     * Keeps accepting clients (if leader) or redirect to leader (if follower)
     */
    public void clientAccept() {
        while (true) {
            try{
                Socket clientSocket = this.clientServerSocket.accept();
                System.out.println("New client connected: " + clientSocket.getInetAddress());

                try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
                    try (PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)){
                        ConnectionMessage msg = (ConnectionMessage) GsonDeserializer.deserialize(in.readLine()); //receive the connection message

                        if (BrokerSettings.getBrokerStatus() !=BrokerStatus.Leader) {
                            //I am NOT the leader so i can't connect with the client.
                            //Send back the IP/Port of the leader
                            var leaderAddress = BrokerSettings.getLeaderAddress();
                            out.println(new ConnectionResponse((String) leaderAddress[0],(Integer) leaderAddress[1]).toJson());
                            //Close the socket
                            continue; //do not submit in the clientsPool
                        }

                        //Handle the assign of an ID.
                        //Send back the client ID. (nextClientId++)
                        var newClientId = msg.getClientID() == -1 ? nextClientId++ : msg.getClientID();
                        out.println(new ConnectionResponse(newClientId, BrokerSettings.getBrokers()).toJson());
                    }
                } catch (IOException e) {
                    System.out.println("Error while establishing client connection: " + e.getMessage());
                }

                // Start a new thread to handle the client
                clientsPool.submit(() -> handleReceivedMessage(clientSocket,true));
            } catch (IOException e) {
                System.out.println("Error while waiting for client connection");
            }
        }
    }

    /**
     * Function run on an endless loop thread.
     * Keeps accepting brokers for possible Candidate message, and connects with the leader(if follower)
     * IS LEADER: keeps the connection to every follower to send periodic (possible empty, works as heartbeat) AppendEntries
     */
    public void brokerAccept() {
        while (true) {
            try {
                Socket brokerSocket = this.brokerServerSocket.accept();
                System.out.println("New broker connected: " + brokerSocket.getInetAddress());

                // Start a new thread to handle the client
                brokersPool.submit(() -> handleReceivedMessage(brokerSocket, false));
            } catch (IOException e) {
                System.out.println("Error while waiting for broker connection");
            }
        }
    }

    /**
     * Function run on a separate thread.
     * Handles the message arriving from that client or broker,
     * adds the message to the queue right queue depending on the fact it was from a broker or from a client
     */
    private void handleReceivedMessage(Socket socket, boolean isClient) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String incomingMessage;
            while ((incomingMessage = in.readLine()) != null) {
                if (isClient) {
                    QueueCommand cmd = (QueueCommand) GsonDeserializer.deserialize(incomingMessage);
                    // Add the message to the shared queue of clients
                    commandsQueue.put(cmd);
                } else {
                    BaseDeserializableMessage cmd = GsonDeserializer.deserialize(incomingMessage);
                    // Add the message to the shared queue of brokers
                    raftCommandsQueue.put(cmd);
                }
            }
        } catch (IOException e) {
            System.out.println("Client disconnected: " + socket.getInetAddress());
        } catch (InterruptedException e) {
            System.out.println("Exception while waiting for queue to free space");
        }
    }

    /**
     * Function that runs in an endless thread loop.
     * Take commands from queue to be processed
     */
    private void processClientsCommand() {
        while (true) {
            try {
                // Take command from the queue and process them
                var command = commandsQueue.take();

                //If a command is here this means this broker is the leader
                //RAFT:
                //1) Append the command to your Log
                //2) Send AppendEntries to every other broker
                //3) !Once I receive the majority of ACK! apply to my internal Model
                BrokerModel.getInstance().processCommand(command);
                //4) After AKCs and after applying the command is COMMITTED: RETURN response to the client
            } catch (InterruptedException e) {
                System.out.println("Exception while waiting for new commands");
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Function that runs in an endless thread loop.
     * Take commands from brokers queue to be processed (manages raft)
     */
    private void processBrokerCommand() {
        while (true) {
            try {
                // Take command from the queue and process them
                var command = raftCommandsQueue.take();
                //TODO: PROCESS THE RAFT COMMAND
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
