package it.distributedsystems.raft;

import it.distributedsystems.connection.ReceiveJsonMessageCallback;
import it.distributedsystems.connection.handler.SocketHandler;
import it.distributedsystems.messages.raft.AppendEntries;
import it.distributedsystems.messages.raft.LeaderIdentification;
import it.distributedsystems.messages.raft.RequestVote;
import it.distributedsystems.utils.BrokerAddress;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * This class represent the follower. Used when the current node is the Leader.
 */
public class Follower {
    //region Volatile state on leader, initialized after every election
    /**
     * index of the next log entry
     *  to send to that server
     *  (initialized to LeaderLastLogIndex+1)
     */
    private int nextIndex;

    /**
     * index of highest log entry
     *  known to be replicated on server
     *  (initialized to 0,increases monotonically)
     */
    private int matchIndex;
    //endregion

    /**
     * The broker id (Follower)
     */
    private final BrokerAddress followerAddress;

    private SocketHandler followerHandler;

    /**
     * This is the function called after receiving a message
     */
    private final ReceiveJsonMessageCallback msgReceiveCallback;

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
            followerHandler.sendMessage(new LeaderIdentification(BrokerSettings.getBrokerID(),BrokerState.getCurrentTerm(),
                    ReplicationLog.getLastLogLineIndex()));

            this.nextIndex = ReplicationLog.getLastLogLineIndex() + 1;
            this.matchIndex = 0;

            //start the socketHandler Listening thread
            new Thread(followerHandler).start();

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public void sendAppendEntries(AppendEntries appendEntries) {
        if (connectHandler()) {
            if (appendEntries == null) { throw new RuntimeException("Send null append entries");}
            if (appendEntries.getFirstNewLineIndex() == nextIndex) {
                followerHandler.sendMessage(appendEntries);
            } else {
                //Create custom appendEntries.
                System.out.println("Reply to append entries for follower " + followerAddress.id + "did not arrive in time, create a new append entries");

                var prevAndNewLog = ReplicationLog.getLogsFromStartIndex(nextIndex-1);
                if (prevAndNewLog.isEmpty()) {
                    System.out.println("GetLogsFromStartIndex returned empty logs for " + (nextIndex -1) + "in Follower " + followerAddress.id);
                }
                var newAppendEntries = new AppendEntries(appendEntries.getLeaderTerm(),appendEntries.getLeaderID());
                newAppendEntries.setLeaderCommitIndex(appendEntries.getLeaderCommitIndex());
                newAppendEntries.setPrevLogTerm(prevAndNewLog.getFirst().getTerm());
                newAppendEntries.setPrevLogIndex(prevAndNewLog.getFirst().getIndex());
                newAppendEntries.addNewLogLine(prevAndNewLog.subList(1, prevAndNewLog.size()));
            }
        }
    }

    public int getFollowerId(){
        return followerAddress.id;
    }

    public boolean isConnected(){
        return followerHandler != null && followerHandler.isConnected();
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

    public void decreaseNextIndex() {
        //TODO: Raft paper proposes an optimization to decrease not by 1, but
        // send some metadata on NACK and set it in a more precise but complex way

        //Keeps track of the next index to send. So at startup will be 1 (0+1), never go less than 1.
        if (this.nextIndex == 1) return; //do not go less than 1
        this.nextIndex = this.nextIndex - 1;
    }

    public void increaseIndexes(int lastReceivedIndex) {
        this.matchIndex = lastReceivedIndex;
        this.nextIndex = this.matchIndex + 1;
    }

    public int getMatchIndex() {
        return this.matchIndex;
    }

    public int getNextIndex() {
        return nextIndex;
    }
}
