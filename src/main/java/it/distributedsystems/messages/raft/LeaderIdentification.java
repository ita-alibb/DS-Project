package it.distributedsystems.messages.raft;

import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.MessageDeserializerType;

public class LeaderIdentification extends BaseDeserializableMessage {
    private final int leaderId;
    private final int leaderTerm;
    private final int leaderLastIndex;

    public LeaderIdentification(int leaderId, int leaderTerm, int leaderLastIndex) {
        super(MessageDeserializerType.LEADER_IDENTIFICATION);
        this.leaderId = leaderId;
        this.leaderTerm = leaderTerm;
        this.leaderLastIndex = leaderLastIndex;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public int getLeaderTerm() {
        return leaderTerm;
    }

    public int getLeaderLastIndex() {
        return leaderLastIndex;
    }
}
