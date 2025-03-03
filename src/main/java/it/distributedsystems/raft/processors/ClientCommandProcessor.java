package it.distributedsystems.raft.processors;

import it.distributedsystems.connection.BrokerConnection;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.queue.QueueCommand;
import it.distributedsystems.messages.queue.QueueResponse;
import it.distributedsystems.messages.raft.AppendEntries;
import it.distributedsystems.raft.BrokerSettings;
import it.distributedsystems.raft.BrokerState;
import it.distributedsystems.raft.LogLine;
import it.distributedsystems.raft.ReplicationLog;
import it.distributedsystems.tui.TUIUpdater;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static it.distributedsystems.raft.BrokerSettings.APPEND_ENTRIES_TIME;

public class ClientCommandProcessor implements Runnable {
    private final AppendEntries currentAppendEntries;
    private final Lock currentBatchLock = new ReentrantLock();
    private AtomicBoolean alive = new AtomicBoolean(true);

    /**
     * This queue is filled by other threads when a response must be sent
     */
    private final BlockingQueue<QueueResponse> responseToSendQueue = new LinkedBlockingQueue<>();

    /**
     * Centralized queue that receives every message from the (possibly) different clients
     */
    private final BlockingQueue<QueueCommand> clientCommandsQueue = new LinkedBlockingQueue<>();
    private QueueCommand lastClientCommand = null;
    public String getLastClientCommand() {
        return lastClientCommand != null ? lastClientCommand.toJson() : "";
    }

    private final ExecutorService periodicalAppendEntriesSender = Executors.newSingleThreadExecutor();
    private final ExecutorService sendResponseThread = Executors.newSingleThreadExecutor();

    public ClientCommandProcessor() {
        this.currentAppendEntries = new AppendEntries(BrokerState.getCurrentTerm(),BrokerSettings.getBrokerID());

        periodicalAppendEntriesSender.execute(this::periodicalSend);
        sendResponseThread.execute(this::responseSender);
    }

    /**
     * Thread that process the message
     */
    @Override
    public void run() {
        while (alive.get()) {
            try {
                // Take command from the queue and process them
                lastClientCommand = clientCommandsQueue.take();

                //If a command is here this means this broker is the leader
                //RAFT:
                //1) Append the command to your Log
                LogLine appendedLine = ReplicationLog.leaderAppendCommand(lastClientCommand);
                System.out.println("Log appended");

                //2) !Send AppendEntries to every other broker! AppendEntries is sent periodically, here we create a batch!
                currentBatchLock.lock();
                currentAppendEntries.addNewLogLine(appendedLine);
                currentBatchLock.unlock();
                System.out.println("Log added to batch");
            } catch (InterruptedException e) {
                System.out.println("Exception while waiting for new commands");
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Timed thread that periodically sends Append Entries.
     */
    public void periodicalSend() {
        try{
            while(alive.get()) {
                AppendEntries messageToSend;

                currentBatchLock.lock();
                //Batch of messages set previously
                //Set commitIndex
                currentAppendEntries.setLeaderCommitIndex(BrokerState.getCommitIndex());

                //ensure prev index is first index +1
                var firstNewLineIndex = currentAppendEntries.getFirstNewLineIndex();
                if (firstNewLineIndex != currentAppendEntries.getPrevLogIndex() + 1 ) {
                    System.out.println("Strange behavior " + firstNewLineIndex + "prev index= " + currentAppendEntries.getPrevLogIndex());
                }

                //Clone the append entries, send after releasing the lock.
                messageToSend = new AppendEntries(this.currentAppendEntries);

                //Set prevLog infos for next AppendEntries
                var newPrevLogLine = this.currentAppendEntries.getLastLogLineInBatch();
                if (newPrevLogLine != null) {//if not empty set the prevlog,
                    this.currentAppendEntries.setPrevLogIndex(newPrevLogLine.getIndex());
                    this.currentAppendEntries.setPrevLogTerm(newPrevLogLine.getTerm());
                }// else means that you just sent a heartbeat and not change previous log line

                //clear del batch
                this.currentAppendEntries.clearBatch();

                currentBatchLock.unlock();

                //After releasing the lock, send to all followers
                BrokerConnection.getInstance().forwardAllFollowers(messageToSend);

                TUIUpdater.setLastMessage("Sent AppendEntries");
                //wait 20 seconds
                Thread.sleep(APPEND_ENTRIES_TIME);
            }
        } catch (InterruptedException e) {
            System.out.println("Exception while waiting for new commands");
            Thread.currentThread().interrupt();
        }
    }

    public void responseSender() {
        /*
          3) !Once I receive the majority of ACK! apply to my internal Model
          Done in RaftCommandProcessor
          4) After AKCs and after applying the command is COMMITTED: RETURN response to the client!
          */
        while (alive.get()){
            try{
                var response = responseToSendQueue.take();

                BrokerConnection.getInstance().sendQueueResponseToClient(response);

            } catch (InterruptedException e) {
                System.out.println("Exception while waiting for new responses");
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Call back function called by every ClientHandler upon receiving of a message
     */
    public void handleClientMessageCallback(String jsonMessage) throws InterruptedException {
        QueueCommand cmd = (QueueCommand) GsonDeserializer.deserialize(jsonMessage);
        // Add the message to the shared queue of clients
        clientCommandsQueue.put(cmd);
    }

    /**
     * Call back function called to add a response to the queue
     */
    public void handleResponseQueueCallback(QueueResponse response) {
        // Add the message to the shared queue of clients
        try {
            responseToSendQueue.put(response);
        } catch (InterruptedException e) {
            System.out.println("Cannot add response to queue");
        }
    }

    public void destroy() {
        alive.set(false);
    }
}
