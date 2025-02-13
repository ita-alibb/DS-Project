package it.distributedsystems.commands;

public class ClientCommand extends BaseCommand{
    private CommandType type;
    private String queueKey;
    private Integer data;
    /**
     * Contructor of the command
     *
     * @param senderID the id of the sender
     * @param type the type of command
     * @param queueKey the id of the queue
     * @param data the data to send (ignored for type CreateQueue or AppendData)
     */
    public ClientCommand(String senderID, CommandType type, String queueKey, int data) {
        super(false, senderID);
        this.type = type;
        this.queueKey = null;
        this.data = null;

        if (this.type == CommandType.CREATE_QUEUE || this.type == CommandType.READ_DATA) {
            this.queueKey = queueKey;
        } else if (this.type == CommandType.APPEND_DATA) {
            this.queueKey = queueKey;
            this.data = data;
        }
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
