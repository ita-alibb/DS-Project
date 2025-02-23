package it.distributedsystems.messages.raft;

import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.MessageDeserializerType;

public class RequestVote extends BaseDeserializableMessage {
    public RequestVote() {
        super(MessageDeserializerType.REQUEST_VOTE);
    }
}
