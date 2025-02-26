package it.distributedsystems.messages.raft;

import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.MessageDeserializerType;

public class RequestVoteResponse extends BaseDeserializableMessage {
    private final int brokerId;
    private final int term;

    private final boolean voteGranted;

    public RequestVoteResponse(int brokerId, int term, boolean voteGranted) {
        super(MessageDeserializerType.REQUEST_VOTE_RESPONSE);
        this.brokerId = brokerId;
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public int getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }
}
