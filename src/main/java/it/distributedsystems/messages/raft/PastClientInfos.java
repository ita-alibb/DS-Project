package it.distributedsystems.messages.raft;

import it.distributedsystems.messages.queue.QueueResponse;

public class PastClientInfos {
    private int clientId;
    private int lastCommandId;
    private QueueResponse lastResponse;

    public PastClientInfos(int clientId, int lastCommandId, QueueResponse lastResponse) {
        this.clientId = clientId;
        this.lastCommandId = lastCommandId;
        this.lastResponse = lastResponse;
    }

    public int getClientId() {
        return clientId;
    }

    public int getLastCommandId() {
        return lastCommandId;
    }

    public QueueResponse getLastResponse() {
        return lastResponse;
    }
}
