package it.distributedsystems.raft;

import it.distributedsystems.connection.BrokerConnection;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.raft.AppendEntries;
import it.distributedsystems.messages.raft.AppendEntriesResponse;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
                    case AppendEntries appendEntries : {
                        //Replica tutti i log nel mio log personale.
                        // Manda Ack dell'AppendEntries al Leader
                    }; break;

                    case AppendEntriesResponse appendEntriesResponse : {
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

    /**
     * Call back function called by every ClientHandler upon receiving of a message
     */
    public void handleRaftMessageCallback(String jsonMessage) throws InterruptedException {
         BaseDeserializableMessage cmd = GsonDeserializer.deserialize(jsonMessage);
        // Add the message to the shared queue of clients
        raftCommandsQueue.put(cmd);
    }
}
