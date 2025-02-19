package it.distributedsystems.raft;

import it.distributedsystems.connection.BrokerConnection;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.queue.QueueCommand;
import it.distributedsystems.messages.raft.AppendEntries;
import it.distributedsystems.messages.raft.UniqueMessageIdentifier;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.System.exit;

public class CommandProcessor implements Runnable {
    private final AppendEntries currentBatchMessage;
    private final Lock currentBatchLock = new ReentrantLock();

    /**
     * This map keeps track of messages and Acks (if > BrokerSettings.getNumofNodes /2 send response to client, add to committed messages)
     */
    private final Map<UniqueMessageIdentifier, Integer> ackCounts = new HashMap<>();
    private final Lock ackLock = new ReentrantLock();

    /**
     * Centralized queue that receives every message from the (possibly) different clients
     */
    private final BlockingQueue<QueueCommand> commandsQueue = new LinkedBlockingQueue<>();

    private final ExecutorService periodicalAppendEntriesSender = Executors.newSingleThreadExecutor();

    public CommandProcessor() {
        this.currentBatchMessage = new AppendEntries(BrokerSettings.getBrokerEpoch(),BrokerSettings.getBrokerID());
        periodicalAppendEntriesSender.submit(this::periodicalSend);
    }

    /**
     * Thread that process the message
     */
    @Override
    public void run() {
        while (true) {
            try {
                // Take command from the queue and process them
                var command = commandsQueue.take();

                //If a command is here this means this broker is the leader
                //RAFT:
                //1) Append the command to your Log
                LogLine appendedLine = ReplicationLog.leaderAppendCommand(command);

                //2) !Send AppendEntries to every other broker! AppendEntries is sent periodically, here we create a batch!
                currentBatchLock.lock();
                currentBatchMessage.addNewLogLine(appendedLine);
                currentBatchLock.unlock();

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
     * Timed thread that periodically sends Append Entries.
     */
    public void periodicalSend() {
        try{
            while(true) {
                //wait 20 seconds
                wait(20_000);
                currentBatchLock.lock();
                //Aggiorna il batch di messaggi fatto esternamente con un metodo
                //prima di mandare setta il commit index
                this.currentBatchMessage.setLeaderCommitIndex(leaderCommitIndex?);

                //manda a tutti i broker //Maybe it creates a new thread
                BrokerConnection.getInstance().forwardAllFollowers(new AppendEntries(this.currentBatchMessage));

                //clear del current batch e SETTA IL PREVLOGLINE!
                this.currentBatchMessage.clearBatch();
                ReplicationLog.setPrevLogLineString(this.currentBatchMessage.getNewLogLineBatch().getLast());
                currentBatchLock.unlock();
            }
        } catch (InterruptedException e) {
            exit(-1);
        }
    }

    /**
     * Call back function called by every ClientHandler upon receiving of a message
     */
    public void handleClientMessageCallback(String jsonMessage) throws InterruptedException {
        QueueCommand cmd = (QueueCommand) GsonDeserializer.deserialize(jsonMessage);
        // Add the message to the shared queue of clients
        commandsQueue.put(cmd);
    }
}
