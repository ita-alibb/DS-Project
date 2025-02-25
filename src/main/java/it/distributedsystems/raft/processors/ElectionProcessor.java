package it.distributedsystems.raft.processors;

import it.distributedsystems.connection.BrokerConnection;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.raft.RequestVote;
import it.distributedsystems.messages.raft.RequestVoteResponse;
import it.distributedsystems.raft.BrokerSettings;
import it.distributedsystems.raft.BrokerStatus;
import it.distributedsystems.raft.Follower;
import it.distributedsystems.raft.ReplicationLog;
import it.distributedsystems.tui.TUIUpdater;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ElectionProcessor implements Runnable {
    //Thread handling
    private final ExecutorService executor;
    private final Queue<Future<?>> futureTask = new LinkedList<>();
    private final List<Follower> followers;
    private final AtomicBoolean electionFailed = new AtomicBoolean(false);

    //Response handling
    private final BlockingQueue<RequestVoteResponse> requestVoteResponses = new LinkedBlockingQueue<>();
    private int acceptedCount = 1;//myself
    private int deniedCount = 0;

    public ElectionProcessor(List<Follower> followerHandlers) {
        this.followers = followerHandlers;
        this.executor = Executors.newFixedThreadPool(followerHandlers.size() + 1);
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        while (true) {
            try {
                // Take command from the queue and process them
                var response = requestVoteResponses.take();

                if (response.isAccepted()) {
                    acceptedCount++;
                } else {
                    deniedCount++;
                }

                //Three possible ways of killing this thread:
                if (acceptedCount > BrokerSettings.getNumOfNodes()/2) {//1) leader is elected, will call setLeader that stops the threads
                    TUIUpdater.setLastMessage("YOU ARE ELECTED AS LEADER");
                    //You are ELECTED
                    BrokerConnection.getInstance().setLeader();
                    BrokerConnection.getInstance().resetElectionTimeout();
                } else if (deniedCount > BrokerSettings.getNumOfNodes()/2) {//2) election failed, set electionFailed, father thread will stop everything
                    //KILL, you are not elected
                    electionFailed.set(true);
                }
            } catch (InterruptedException e) {//3) The father thread has been interrupted
                System.out.println("Election interrupted");
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public void handleRequestVoteResponses(String jsonMessage) throws InterruptedException {
        TUIUpdater.setLastMessage("Received request vote responses: " + jsonMessage);
        RequestVoteResponse cmd = (RequestVoteResponse) GsonDeserializer.deserialize(jsonMessage);
        // Add the message to the shared queue
        requestVoteResponses.put(cmd);
    }

    /**
     * This function is called by the thread after the end of the TIMEOUT in BrokerConnection
     */
    public void startElection(){
        try {
            resetElection();

            //send to every node a RequestVote
            var requestVote = new RequestVote(BrokerSettings.getBrokerEpoch(), ReplicationLog.getPrevLogLineIndex(), ReplicationLog.getPrevLogLineTerm());

            for (final Follower follower : followers) {
                futureTask.add(
                        executor.submit(() -> follower.sendElection(requestVote, this::handleRequestVoteResponses))
                );
            }

            //collectResponses, inside the thread it sets the leader if get >N/2 responses.
            futureTask.add(executor.submit(this));

            //Keep waiting for eventually interruption
            while (true) {
                if (Thread.currentThread().isInterrupted() || electionFailed.get()) {
                    //It has been interrupted or election Failed, stop every other child thread
                    interruptExecutor();
                    Thread.currentThread().interrupt();
                    return;//end this threadgf
                }
            }
        } catch (Exception e) {
            System.out.println("Something went wrong in StartElection " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void interruptExecutor(){
        while(!futureTask.isEmpty()) {
            futureTask.poll().cancel(true);
        }
    }

    private void resetElection(){
        //set Candidate
        BrokerSettings.setBrokerStatus(BrokerStatus.Candidate);
        //schedule next timeout in case of tie in this election
        BrokerConnection.getInstance().resetElectionTimeout();
        //set new term for this election
        BrokerSettings.setBrokerEpoch(BrokerSettings.getBrokerEpoch() + 1);
        //reset counters
        acceptedCount = 1;
        deniedCount = 0;
        interruptExecutor();
        //reset election status
        electionFailed.set(false);
    }

    public int getAcceptedCount() {
        return acceptedCount;
    }

    public int getDeniedCount() {
        return deniedCount;
    }
}
