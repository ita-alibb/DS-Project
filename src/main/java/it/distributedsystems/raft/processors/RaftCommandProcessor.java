package it.distributedsystems.raft.processors;

import it.distributedsystems.connection.BrokerConnection;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.raft.AppendEntries;
import it.distributedsystems.messages.raft.AppendEntriesResponse;
import it.distributedsystems.raft.*;
import it.distributedsystems.tui.TUIUpdater;

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
                        TUIUpdater.setLastMessage("AppendEntries received");
                        //Even if the append entries will be refused, the leader is active
                        BrokerSettings.setBrokerStatus(BrokerStatus.Follower);
                        BrokerConnection.getInstance().resetElectionTimeout();
                        BrokerState.setCurrentTerm(appendEntries.getLeaderTerm());

                        if (testAppendEntries(appendEntries)) {
                            //Replicate all log.
                            ReplicationLog.followerLogReconciliation(appendEntries);

                            // Send AppendEntries ACK to Leader
                            BrokerConnection.getInstance().sendMessageToLeader(new AppendEntriesResponse(
                                    BrokerSettings.getBrokerID(),
                                    BrokerState.getCurrentTerm(),
                                    true,
                                    ReplicationLog.getLastLogLineIndex()
                            ));

                            /*If leaderCommit>commitIndex,*/
                            if (appendEntries.getLeaderCommitIndex() > BrokerState.getCommitIndex()) {
                                var lastNewLogIndex = appendEntries.getLastNewLineIndex();

                                //As stated in the paper set commitIndex= min(leaderCommit,index of last new entry)
                                //If there are no new entry consider the current commit index
                                BrokerState.setCommitIndex(Math.min(BrokerState.getCommitIndex(),
                                        (lastNewLogIndex == -1) ? BrokerState.getCommitIndex() : lastNewLogIndex));
                            }
                        } else {
                            // Send AppendEntries NACK to Leader
                            BrokerConnection.getInstance().sendMessageToLeader(new AppendEntriesResponse(
                                    BrokerSettings.getBrokerID(),
                                    BrokerState.getCurrentTerm(),
                                    false,
                                    ReplicationLog.getLastLogLineIndex()
                            ));
                        }
                    }; break;

                    case AppendEntriesResponse appendEntriesResponse : {//Message received by a LEADER
                        //Receive ACK, increase indexes
                        if (appendEntriesResponse.isSuccess()) {
                            //ACK received
                            //Update matchIndex
                            BrokerConnection.getInstance().increaseFollowerIndexes(
                                    appendEntriesResponse.getBrokerId(),
                                    appendEntriesResponse.getLastLogIndex()
                            );

                            //Now that you have changed the match index for a Follower, chech the ocndition to update the commit index of the leaderyt
                            new Thread(this::checkUpdateLeaderCommitIndex);

                        } else {
                            //NACK received
                            // If RPC request or response contains term T>currentTerm:
                            // set current Term = T,convert to follower
                            if (appendEntriesResponse.getCurrentTerm() > BrokerState.getCurrentTerm()) {
                                BrokerState.setCurrentTerm(appendEntriesResponse.getCurrentTerm());
                                BrokerConnection.getInstance().setFollower();
                            }

                            //else decrease the Follower nextIndex
                            BrokerConnection.getInstance().decreaseFollowerNextIndex(appendEntriesResponse.getBrokerId());
                        }
                    }; break;

                    default: //TODO: unexpected message here.
                }

            } catch (InterruptedException e) {
                System.out.println("Exception while waiting for new RAFT commands");
                Thread.currentThread().interrupt();
            }
        }
    }

    private void checkUpdateLeaderCommitIndex() {
        // If there exists an N such that N>commitIndex, a majority
        // of matchIndex[i]≥N, and log[N].term==currentTerm:
        // set commitIndex=N
        var currentMax = -1;
        try{
            for (int N = BrokerState.getCommitIndex(); N < ReplicationLog.getLastLogLineIndex(); N++) {
                var nCount = 0;

                for (Follower f : BrokerConnection.getInstance().getFollowers()) {
                    if (f.getMatchIndex() >= N) {
                        nCount++;
                    }
                }

                var nLog = ReplicationLog.getLog(N);
                if (nCount > BrokerSettings.getNumOfNodes()/2 && nLog != null && nLog.getTerm() == BrokerState.getCurrentTerm()){
                    currentMax = Math.max(N,currentMax);
                }
            }

            if (currentMax > BrokerState.getCommitIndex()) {
                BrokerState.setCommitIndex(currentMax);//When setting the commit index it automatically start a thread that take lastApplied to commitIndex
            }
        } catch (Exception e) {
            System.out.println("Unexpected exception while check update commit index " + e.getMessage());
        }
    }

    private boolean testAppendEntries(AppendEntries appendEntries) {
        if (appendEntries == null) exit(-1);
        if (appendEntries.getLeaderTerm() < BrokerState.getCurrentTerm()) return false;
        if (appendEntries.getPrevLogIndex() == 0 && ReplicationLog.getLastLogLineIndex() == 0) return true;//special case, log file is empty for both leader and follower, the follower cannot check the log 0.
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
