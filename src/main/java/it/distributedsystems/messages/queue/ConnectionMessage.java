package it.distributedsystems.messages.queue;

/**
 * This class represent the base message sent from client to Brokers, it contains the clientID.
 * It is also the class used to establish the connection to sync the clientID.
 */
public class ConnectionMessage extends BaseDeserializableMessage {
    /**
     * The id of the sender of the message
     */
    private final int clientID;

    /**
     * Constructor of the command
     * @param clientID the id of the sender
     */
    public ConnectionMessage(int clientID) {
        super(MessageDeserializerType.CONNECTION_MESSAGE);
        this.clientID = clientID;
    }

    /**
     * Constructor used to set the type for inheritors
     */
    public ConnectionMessage(int clientID, MessageDeserializerType type) {
        super(type);
        this.clientID = clientID;
    }

    public int getClientID() {
        return clientID;
    }
}
