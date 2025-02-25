package it.distributedsystems.raft;

import it.distributedsystems.connection.ReceiveJsonMessageCallback;
import it.distributedsystems.connection.handler.SocketHandler;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.raft.AppendEntries;
import it.distributedsystems.messages.raft.LeaderIdentification;
import it.distributedsystems.messages.raft.RequestVote;
import it.distributedsystems.utils.BrokerAddress;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.LinkedList;

/**
 * This class represent the follower. Used when the current node is the Leader.
 */
public class Follower {
    /**
     * The broker id (Follower)
     */
    private final BrokerAddress followerAddress;

    /**
     * This is the next index to send.
     * It is used to keep track of how much AppendEntries you have to send.
     * Updated after receiving ACK
     */
    private int nextIndex;

    private SocketHandler followerHandler;

    /**
     * This is the function called after receiving a message
     */
    private final ReceiveJsonMessageCallback msgReceiveCallback;

    /**
     * Queue of lost AppendEntries. On connection empty by sending every element
     */
    private final LinkedList<AppendEntries> lostAppendEntries = new LinkedList<>();

    public Follower(BrokerAddress address, ReceiveJsonMessageCallback msgReceiveCallback) {
        this.followerAddress = address;
        this.nextIndex = 0;
        this.msgReceiveCallback = msgReceiveCallback;
    }

    /**
     * Connect the followerHandler (client) to the offered ServerSocket of the Follower node.
     * Starts the listening thread on the followerHandler (SocketHandler)
     */
    public boolean connectHandler() {
        // already connected
        if (isConnected()) return true;

        //Connection protocol Leader->Follower, leader side.
        try {
            var followerSocket = new Socket(followerAddress.IP,followerAddress.BrokerServerPort);

            //create the handler
            followerHandler = new SocketHandler(followerSocket, msgReceiveCallback);

            //send to the handler the message to identify as the Leader
            followerHandler.sendMessage(new LeaderIdentification(BrokerSettings.getBrokerID(),BrokerSettings.getBrokerEpoch(), ReplicationLog.getPrevLogLineString().getIndex()));

            //send every lost appendEntries
            while (!lostAppendEntries.isEmpty()) {
                followerHandler.sendMessage(lostAppendEntries.pop());
            }

            //start the socketHandler Listening thread
            new Thread(followerHandler).start();

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public void sendMessage(BaseDeserializableMessage message) {
        if (connectHandler()) {
            //TODO: qui ci sono dei check da fare mi sa. Ed update degli index
            followerHandler.sendMessage(message);
        } else {
            if (message instanceof AppendEntries) {
                lostAppendEntries.add((AppendEntries) message);
            }
            System.out.println("Follower" + followerAddress.id + " is not connected, message not sent");
        }
    }

    public int getFollowerId(){
        return followerAddress.id;
    }

    public boolean isConnected(){
        return !(followerHandler == null || followerHandler.isConnected());
    }

    /**
     * Called on election in a new thread
     */
    public void sendElection(RequestVote requestVote, ReceiveJsonMessageCallback handleResponse) {
        //Connection protocol Leader->Follower, leader side.
        try (var followerSocket = new Socket(followerAddress.IP,followerAddress.BrokerServerPort)) {
            var out = new PrintWriter(followerSocket.getOutputStream(), true);
            var in = new BufferedReader(new InputStreamReader(followerSocket.getInputStream()));

            //send to the handler the message to identify as the Leader
            out.println(requestVote.toJson());

            handleResponse.onReceiveJsonMessage(in.readLine());
        } catch (InterruptedException | IOException e) {
            if (Thread.currentThread().isInterrupted()) {
                System.out.println("Election thread has been interrupted, interrupt waiting on follower " + followerAddress.id);
            } else {
                System.out.println("IOException occurred while waiting for election response on follower: " + followerAddress.id);
            }
            Thread.currentThread().interrupt();
            return;
        }
    }
}
