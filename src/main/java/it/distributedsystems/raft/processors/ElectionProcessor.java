package it.distributedsystems.raft.processors;

import it.distributedsystems.connection.BrokerConnection;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.raft.RequestVote;
import it.distributedsystems.messages.raft.RequestVoteResponse;
import it.distributedsystems.raft.*;
import it.distributedsystems.tui.TUIUpdater;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ElectionProcessor implements Runnable {
    //Thread handling
    private final ExecutorService executor;
    private final Queue<Future<?>> futureTask = new LinkedList<>();
    private final List<Follower> followers;
    //private final AtomicBoolean electionFailed = new AtomicBoolean(false);

    //Response handling
    private final BlockingQueue<RequestVoteResponse> requestVoteResponses = new LinkedBlockingQueue<>();
    private final Set<Integer> acceptedCount = new HashSet<>();
    private final Set<Integer> deniedCount = new HashSet<>();

    public ElectionProcessor(List<Follower> followerHandlers) {
        this.followers = followerHandlers;
        this.executor = Executors.newFixedThreadPool(followerHandlers.size() + 1);
        acceptedCount.add(BrokerSettings.getBrokerID());//add myself
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

                if (response.isVoteGranted()) {
                    acceptedCount.add(response.getBrokerId());
                } else {
                    deniedCount.add(response.getBrokerId());
                }

                //Three possible ways of killing this thread:
                if (acceptedCount.size() > BrokerSettings.getNumOfNodes()/2) {//1) leader is elected, will call setLeader that stops the threads
                    TUIUpdater.setLastMessage("YOU ARE ELECTED AS LEADER");
                    //You are ELECTED
                    BrokerConnection.getInstance().setLeader();
                }// else if (deniedCount.size() > BrokerSettings.getNumOfNodes()/2) {//2) election failed, set electionFailed, father thread will stop everything
                    //You are not elected, let the election expire to add some time interval
                    //electionFailed.set(false);
                //}
            } catch (InterruptedException e) {//3) The father thread has been interrupted
                System.out.println("Election interrupted");
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public void handleRequestVoteResponses(String jsonMessage) throws InterruptedException {
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
            var requestVote = new RequestVote(BrokerSettings.getBrokerID(), BrokerState.getCurrentTerm(), ReplicationLog.getLastLogLineIndex(), ReplicationLog.getLastLogLineTerm());

            for (final Follower follower : followers) {
                futureTask.add(
                        executor.submit(() -> follower.sendElection(requestVote, this::handleRequestVoteResponses))
                );
            }

            //collectResponses, inside the thread it sets the leader if get >N/2 responses.
            futureTask.add(executor.submit(this));

            //Keep waiting for eventually interruption
            while (true) {
                if (Thread.currentThread().isInterrupted()/* || electionFailed.get()*/) {
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
        BrokerState.setCurrentTerm(BrokerState.getCurrentTerm() + 1);
        BrokerState.setVotedFor(BrokerSettings.getBrokerID());
        //reset counters
        acceptedCount.clear();
        acceptedCount.add(BrokerSettings.getBrokerID());
        deniedCount.clear();
        interruptExecutor();
        //reset election status
        //electionFailed.set(false);
    }

    public Set<Integer> getAcceptedCount() {
        return acceptedCount;
    }

    public Set<Integer> getDeniedCount() {
        return deniedCount;
    }
}
