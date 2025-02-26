package it.distributedsystems.raft.processors;

import it.distributedsystems.connection.BrokerConnection;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.raft.AppendEntries;
import it.distributedsystems.messages.raft.AppendEntriesResponse;
import it.distributedsystems.raft.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.System.exit;

public class RaftCommandProcessor implements Runnable{
    /**
     * Centralized queue that receives every message from the Brokers.
     * If I am the leader I can receive only ACKs from other Followers.
     * If I am a follower I can receive only AppendEntries (the CommitConfirmation is intrinsic in the next AppendEntries, you will get the eventually updated commitIndex) from Leader.
     * During errors:
     * A leader can receive a NACK from a follower. In that case a special AppendEntries will be sent to him.
     */
    private final BlockingQueue<BaseDeserializableMessage> raftCommandsQueue = new LinkedBlockingQueue<>();

    public RaftCommandProcessor() {
    }

    /**
     * Thread that process the message
     */
    @Override
    public void run() {
        while (true) {
            try {
                // Take command from the queue and process them
                var command = raftCommandsQueue.take();

                switch (command) {
                    case AppendEntries appendEntries : {//Message received by a FOLLOWER
                        // TODO: IF APPENDENTRIES IS VALID:
                        //resetta il timer e settati come FOLLOWER (ricevere ED ACCETTARE un AppendEntries fa di te un follower)
                        //If the timer is reset means that you received an AppendEntries, so you are a follower
                        if (testAppendEntries(appendEntries)) {
                            BrokerSettings.setBrokerStatus(BrokerStatus.Follower);
                            BrokerConnection.getInstance().resetElectionTimeout();

                            //Replica tutti i log nel mio log personale.
                            ReplicationLog.followerCopyAppendEntriesLog(appendEntries);

                            // Manda Ack dell'AppendEntries al Leader

                            /*If leaderCommit>commitIndex,*/
                            if (appendEntries.getLeaderCommitIndex() > BrokerState.getCommitIndex()) {
                                var lastNewLogIndex = appendEntries.getLastNewLineIndex();

                                //As stated in the paper set commitIndex= min(leaderCommit,index of last new entry)
                                //If there are no new entry consider the current commit index
                                BrokerState.setCommitIndex(Math.min(BrokerState.getCommitIndex(),
                                        (lastNewLogIndex == -1) ? BrokerState.getCommitIndex() : lastNewLogIndex));
                            }
                        } else {

                            //TODO: IF not valid send NACK
                        }
                    }; break;

                    case AppendEntriesResponse appendEntriesResponse : {//Message received by a LEADER
                        //Il leader riceve qui l'ACK.
                        //Aumenta il counter di ACK in ClientCommandProcessor.(callback o una chiamata tramite BrokerConnection)
                    }; break;

                    default: //TODO: unexpected message here.
                }

            } catch (InterruptedException e) {
                System.out.println("Exception while waiting for new RAFT commands");
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean testAppendEntries(AppendEntries appendEntries) {
        if (appendEntries == null) exit(-1);
        if (appendEntries.getLeaderTerm() < BrokerState.getCurrentTerm()) return false;
        LogLine prevLogLine = ReplicationLog.getLog(appendEntries.getPrevLogIndex());
        if (prevLogLine == null || prevLogLine.getTerm() != appendEntries.getPrevLogTerm()) return false;
        return true;
    }
    /**
     * Call back function called by every ClientHandler upon receiving of a message
     */
    public void handleRaftMessageCallback(String jsonMessage) throws InterruptedException {
         BaseDeserializableMessage cmd = GsonDeserializer.deserialize(jsonMessage);
        // Add the message to the shared queue of clients
        raftCommandsQueue.put(cmd);
    }
}
