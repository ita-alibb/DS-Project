package it.distributedsystems.messages;

import com.google.gson.Gson;

public abstract class BaseCommand {
    private static int counter = 0;
    /**
     * Every instance of the base command will have a unique Id
     * (unique for every sender e.g. every client, senderID+commandID will be globally unique)
     */
    public final int commandId;
    /**
     * The id of the sender of the message
     */
    private int senderID;

    /**
     * Bool indication whether the message is from a broker
     */
    private boolean isBroker;

    /**
     * Contructor of the command
     * @param isBroker if is from a broker or not
     * @param senderID the id of the sender
     */
    public BaseCommand(boolean isBroker, int senderID) {
        this.commandId = ++counter;
        this.isBroker = isBroker;
        this.senderID = senderID;
    }

    /**
     * Method to convert current class to json
     * @return the String of json representation
     */
    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public int getSenderID() {
        return senderID;
    }

    public boolean isBroker() {
        return isBroker;
    }
}
