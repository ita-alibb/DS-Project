package it.distributedsystems.messages.client;

import it.distributedsystems.messages.BaseCommand;

public class ClientCommand extends BaseCommand {
    private CommandType type;
    private String queueKey;
    private Integer data;
    /**
     * Contructor of the command
     *
     * @param senderID the id of the sender
     * @param type the type of command
     * @param queueKey the id of the queue
     * @param data the data to send (ignored for type CreateQueue or ReadData)
     */
    public ClientCommand(int senderID, CommandType type, String queueKey, Integer data) {
        super(false, senderID);
        this.type = type;
        this.queueKey = queueKey;
        this.data = data; // not null only for AppendData
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
