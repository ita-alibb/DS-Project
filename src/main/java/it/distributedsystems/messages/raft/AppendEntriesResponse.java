package it.distributedsystems.messages.raft;

import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.MessageDeserializerType;

public class AppendEntriesResponse extends BaseDeserializableMessage {
    private final int currentTerm; //Term used by the follower to update the leader if it results not as up to date as the follower
    private final boolean success; //If True it is a ACK, if false it is not accepted

    public AppendEntriesResponse(int term, boolean success) {
        super(MessageDeserializerType.APPEND_ENTRIES_RESPONSE);
        this.currentTerm = term;
        this.success = success;
    }
    public int getCurrentTerm() {
        return currentTerm;
    }
    public boolean isSuccess() {
        return success;
    }
}
