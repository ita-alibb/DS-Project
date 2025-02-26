package it.distributedsystems.raft.processors;

import it.distributedsystems.connection.BrokerConnection;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.queue.QueueCommand;
import it.distributedsystems.messages.raft.AppendEntries;
import it.distributedsystems.messages.raft.AppendEntriesResponse;
import it.distributedsystems.messages.raft.UniqueMessageIdentifier;
import it.distributedsystems.raft.BrokerSettings;
import it.distributedsystems.raft.BrokerState;
import it.distributedsystems.raft.LogLine;
import it.distributedsystems.raft.ReplicationLog;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static it.distributedsystems.raft.BrokerSettings.APPEND_ENTRIES_TIME;
import static java.lang.System.exit;

public class ClientCommandProcessor implements Runnable {
    private final AppendEntries currentAppendEntries;
    private final Lock currentBatchLock = new ReentrantLock();

    /**
     * This map keeps track of messages and Acks (if > BrokerSettings.getNumofNodes /2 send response to client, add to committed messages)
     */
    private final Map<UniqueMessageIdentifier, Integer> ackCounts = new HashMap<>();
    private final Lock ackLock = new ReentrantLock();

    /**
     * Centralized queue that receives every message from the (possibly) different clients
     */
    private final BlockingQueue<QueueCommand> clientCommandsQueue = new LinkedBlockingQueue<>();
    private QueueCommand lastClientCommand = null;
    public String getLastClientCommand() {
        return lastClientCommand != null ? lastClientCommand.toJson() : "";
    }

    private final ExecutorService periodicalAppendEntriesSender = Executors.newSingleThreadExecutor();

    public ClientCommandProcessor() {
        this.currentAppendEntries = new AppendEntries(BrokerState.getCurrentTerm(),BrokerSettings.getBrokerID());

        periodicalAppendEntriesSender.execute(this::periodicalSend);
    }

    /**
     * Thread that process the message
     */
    @Override
    public void run() {
        while (true) {
            try {
                // Take command from the queue and process them
                lastClientCommand = clientCommandsQueue.take();

                //If a command is here this means this broker is the leader
                //RAFT:
                //1) Append the command to your Log
                LogLine appendedLine = ReplicationLog.leaderAppendCommand(lastClientCommand);

                //2) !Send AppendEntries to every other broker! AppendEntries is sent periodically, here we create a batch!
                currentBatchLock.lock();
                currentAppendEntries.addNewLogLine(appendedLine);
                currentBatchLock.unlock();

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
                currentBatchLock.lock();
                //Aggiorna il batch di messaggi fatto esternamente con un metodo
                //TODO: prima di mandare setta il commit index
                //TODO: this.currentBatchMessage.setLeaderCommitIndex(leaderCommitIndex?);
                //ensure prev index is first index +1
                var firstNewLineIndex = currentAppendEntries.getLastNewLineIndex();
                if (firstNewLineIndex != currentAppendEntries.getPrevLogIndex() + 1 ) {
                    System.out.println("Strange behavior " + firstNewLineIndex + "prev index= " + currentAppendEntries.getPrevLogIndex());
                }

                //manda a tutti I follower
                BrokerConnection.getInstance().forwardAllFollowers(new AppendEntries(this.currentAppendEntries));

                //Set prevLog infos for next AppendEntries
                var newPrevLogLine = this.currentAppendEntries.getLastLogLineInBatch();
                if (newPrevLogLine != null) {//if not empty set the prevlog
                    this.currentAppendEntries.setPrevLogIndex(newPrevLogLine.getIndex());
                    this.currentAppendEntries.setPrevLogTerm(newPrevLogLine.getTerm());
                }

                //clear del batch
                this.currentAppendEntries.clearBatch();


                currentBatchLock.unlock();

                //wait 20 seconds
                Thread.sleep(APPEND_ENTRIES_TIME);
            }
        } catch (InterruptedException e) {
            exit(-1);
        }
    }

    public void registerAck(AppendEntriesResponse response) {
        //per ogni messaggio presente nell AppendEntries, se la risposta e' positiva aggiungi al counter degli Ack, per ogni comando,
        //l'id del follower che ti ha appena fatto ACK (aggiungi id in un set, cosi' se per caso ne manda 2 comunque non cambia la somma)
        // poi SE ogni precedente comando e' committato ed il presente comando ha >N/2 ACK allora committa anche questo.
        //Quindi un while sul peek di una coda!.
        //operazione da performare per ogni comando da committare:
        /*
          3) !Once I receive the majority of ACK! apply to my internal Model
          var response = BrokerModel.getInstance().processCommand(command);
          4) After AKCs and after applying the command is COMMITTED: RETURN response to the client AND UPDATE COMMITTEDINDEX!
          BrokerConnection.getInstance().sendQueueResponseToClient(response);*/
    }

    /**
     * Call back function called by every ClientHandler upon receiving of a message
     */
    public void handleClientMessageCallback(String jsonMessage) throws InterruptedException {
        QueueCommand cmd = (QueueCommand) GsonDeserializer.deserialize(jsonMessage);
        // Add the message to the shared queue of clients
        clientCommandsQueue.put(cmd);
    }
}
