package it.distributedsystems.messages.raft;

import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.MessageDeserializerType;

public class AppendEntriesResponse extends BaseDeserializableMessage {
    private boolean error; //If True it is a NACK, if false it is an ACK

    public AppendEntriesResponse() {
        super(MessageDeserializerType.APPEND_ENTRIES_RESPONSE);
    }
}
