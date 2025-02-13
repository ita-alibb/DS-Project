package it.distributedsystems.commands;

import com.google.gson.Gson;

public abstract class BaseCommand {
    /**
     * The id of the sender of the message
     */
    private String senderID;

    /**
     * Bool indication whether the message is from a broker
     */
    private boolean isBroker;

    /**
     * Contructor of the command
     * @param isBroker if is from a broker or not
     * @param senderID the id of the sender
     */
    public BaseCommand(boolean isBroker, String senderID) {
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

    public String getSenderID() {
        return senderID;
    }

    public boolean isBroker() {
        return isBroker;
    }
}
