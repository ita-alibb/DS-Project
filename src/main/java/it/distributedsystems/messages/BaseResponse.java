package it.distributedsystems.messages;

import com.google.gson.Gson;

public class BaseResponse {
    /**
     * The id of the sender of the message
     */
    private int clientId;

    /**
     * The id of the command to which the response refers
     */
    private int commandId;

    /**
     * Status, if null al good
     */
    public String status;

    /**
     * If true a string is passed as status, something went wrong
     */
    public boolean error;

    public BaseResponse(int clientId, int commandId) {
        this.clientId = clientId;
        this.commandId = commandId;
        this.status = null;
        this.error = false;
    }

    public BaseResponse(int clientId, int commandId, String status, boolean error) {
        this.clientId = clientId;
        this.commandId = commandId;
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

    public int getClientId() {
        return clientId;
    }

    public int getCommandId() {
        return commandId;
    }

    public String getStatus() {
        return status;
    }

    public boolean isError() {
        return error;
    }
}
