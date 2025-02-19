package it.distributedsystems.messages.queue;

import it.distributedsystems.messages.MessageDeserializerType;

/**
 * This class represent the command sent from the client to the Leader to operate in the queues
 */
public class QueueCommand extends ConnectionMessage {
    private static int counter = 0;
    /**
     * Every instance of the base command will have a unique Id
     * (unique for every sender e.g. every client, senderID+commandID will be globally unique)
     */
    public final int commandID;

    private final CommandType type;
    private final String queueKey;
    private final Integer data;


    /**
     * Constructor of the command
     * @param clientID the id of the sender
     * @param type the type of command
     * @param queueKey the id of the queue
     * @param data the data to send (ignored for type CreateQueue or ReadData)
     */
    public QueueCommand(int clientID, CommandType type, String queueKey, Integer data) {
        super(clientID, MessageDeserializerType.QUEUE_COMMAND);
        this.commandID = ++counter; // assign the automatic incrementing unique commandID
        this.type = type;
        this.queueKey = queueKey;
        this.data = data; // not null only for AppendData
    }

    public int getCommandID() {
        return commandID;
    }

    public CommandType getType() {
        return type;
    }

    public String getQueueKey() {
        return queueKey;
    }

    public Integer getData() {
        return data;
    }
}
