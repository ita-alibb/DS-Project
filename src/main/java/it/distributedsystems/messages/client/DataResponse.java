package it.distributedsystems.messages.client;

import it.distributedsystems.messages.BaseResponse;

public class DataResponse extends BaseResponse {
    private Integer data;

    public DataResponse(int clientId, int commandId, String status, boolean error) {
        super(clientId, commandId, status, error);
        this.data = null;
    }

    public DataResponse(int clientId, int commandId, int data) {
        super(clientId, commandId);
        this.data = data;
    }

    public Integer getData() {
        return data;
    }
}
