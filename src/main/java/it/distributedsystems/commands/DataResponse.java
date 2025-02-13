package it.distributedsystems.commands;

public class DataResponse extends BaseResponse{
    private int data;

    public DataResponse(String senderID, String status, boolean error) {
        super(senderID, status, error);
        this.data = Integer.MIN_VALUE;
    }

    public DataResponse(String senderID, String status, boolean error, int data) {
        super(senderID, status, error);
        this.data = data;
    }

    public int getData() {
        return data;
    }
}
