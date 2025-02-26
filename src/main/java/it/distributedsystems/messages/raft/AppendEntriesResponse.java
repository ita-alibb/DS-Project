package it.distributedsystems.messages.raft;

import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.MessageDeserializerType;

public class AppendEntriesResponse extends BaseDeserializableMessage {
    private final int brokerId;
    private final int currentTerm; //Term used by the follower to update the leader if it results not as up to date as the follower
    private final boolean success; //If True it is a ACK, if false it is not accepted
    private final int lastLogIndex; // the last log index from the follower

    public AppendEntriesResponse(int brokerId, int term, boolean success, int lastLogIndex) {
        super(MessageDeserializerType.APPEND_ENTRIES_RESPONSE);
        this.brokerId = brokerId;
        this.currentTerm = term;
        this.success = success;
        this.lastLogIndex = lastLogIndex;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }
}
