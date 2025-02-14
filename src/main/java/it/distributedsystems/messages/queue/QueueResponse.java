package it.distributedsystems.messages.queue;

import com.google.gson.Gson;

/**
 * This class represent the response sent from the Leader to the client after a QueueCommand.
 * Does not contain the clientID because if the client receives it, this means is for him.
 */
public class QueueResponse extends BaseDeserializableMessage {
    /**
     * The id of the command to which the response refers
     */
    private final int commandId;

    /**
     * If not null something went wrong
     */
    public String error;

    /**
     * The data of the response, not null only if the commandID is for a ReadData
     */
    private final Integer data;

    /**
     * Error response Constructor
     */
    public QueueResponse(int commandId, String error) {
        super(MessageDeserializerType.QUEUE_RESPONSE);
        this.commandId = commandId;
        this.error = error;
        this.data = null;
    }

    /**
     * Positive response constructor, data may be null
     */
    public QueueResponse(int commandId, Integer data) {
        super(MessageDeserializerType.QUEUE_RESPONSE);
        this.commandId = commandId;
        this.data = data;
        this.error = null;
    }

    /**
     * Method to convert current class to json
     * @return the String of json representation
     */
    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public int getCommandId() {
        return commandId;
    }

    public String getError() {
        return error;
    }

    public Integer getData() {
        return data;
    }
}
