package it.distributedsystems.messages.raft;

import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.MessageDeserializerType;

public class RequestVoteResponse extends BaseDeserializableMessage {
    private final int followerID;

    private final boolean accepted;

    public RequestVoteResponse(int followerId, boolean accepted) {
        super(MessageDeserializerType.REQUEST_VOTE_RESPONSE);
        this.accepted = accepted;
        this.followerID = followerId;
    }

    public int getFollowerID() {
        return followerID;
    }

    public boolean isAccepted() {
        return accepted;
    }
}
