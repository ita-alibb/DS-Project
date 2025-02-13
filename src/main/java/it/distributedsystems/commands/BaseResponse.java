package it.distributedsystems.commands;

import com.google.gson.Gson;

public class BaseResponse {
    /**
     * The id of the sender of the message
     */
    private String senderID;

    /**
     * Status, if null al good
     */
    public String status;

    /**
     * If true a string is passed as status, something went wrong
     */
    public boolean error;

    public BaseResponse(String senderID) {
        this.senderID = senderID;
        this.status = null;
        this.error = false;
    }

    public BaseResponse(String senderID, String status, boolean error) {
        this.senderID = senderID;
        this.status = status;
        this.error = error;
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

    public String getStatus() {
        return status;
    }

    public boolean isError() {
        return error;
    }
}
