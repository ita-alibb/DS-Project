package it.distributedsystems.connection;

import it.distributedsystems.messages.BaseCommand;
import it.distributedsystems.messages.GsonDeserializer;
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
    private final BlockingQueue<BaseCommand> commandsQueue = new LinkedBlockingQueue<>();

    /**
     * Centralized queue that receives every message from the Brokers
     */
    private final BlockingQueue<BaseCommand> raftCommandsQueue = new LinkedBlockingQueue<>();

    public BrokerConnection(int tcpPort) {
        try{
            this.clientServerSocket = new ServerSocket(tcpPort);
            System.out.println("Server Socket for CLIENTS started on port " + tcpPort);

            this.brokerServerSocket = new ServerSocket(tcpPort+1);
            System.out.println("Server Socket for BROKERS started on port " + (tcpPort+1));

            //Start the accepting thread on clientServerSocket and brokerServerSocket
            acceptancePool.submit(this::clientAccept);
            acceptancePool.submit(this::brokerAccept);

            //TODO: forse per i broker non c'e' bisogno di avere il thread pool? perhe' in genere i follower ti mandano un solo messaggio, pero' vabbe' sticazzi lo fai copia incollato da quello dei client e sticazzoloni

            // Start a separate thread to process messages from all clients and brokers
            processPool.submit(this::processClientsCommand);

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

                if (!BrokerStatus.Leader) {
                    //I am NOT the leader so i can't connect with the client.
                    //Send back the IP/Port of the leader
                    //Close the socket
                    continue; //do not submit in the clientsPool
                }

                //Handle the assign of an ID.
                //Send back the client ID. (nextClientId++)


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
     * IF LEADER: keeps the connection to every follower to send periodic (possible empty, works as heartbeat) AppendEntries
     */
    public void brokerAccept() {
        while (true) {
            Socket brokerSocket = this.brokerServerSocket.accept();
            System.out.println("New broker connected: " + brokerSocket.getInetAddress());

            // Start a new thread to handle the client
            brokersPool.submit(() -> handleReceivedMessage(brokerSocket, false));
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
                    BaseCommand cmd = GsonDeserializer.deserializeCommand(incomingMessage);
                    // Add the message to the shared queue of clients
                    commandsQueue.add(cmd);
                } else {
                    BaseCommand cmd = GsonDeserializer.deserializeCommand(incomingMessage);
                    // Add the message to the shared queue of brokers
                    raftCommandsQueue.add(cmd);
                }
            }
        } catch (IOException e) {
            System.out.println("Client disconnected: " + socket.getInetAddress());
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
                //TODO: PROCESS THE COMMAND
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
