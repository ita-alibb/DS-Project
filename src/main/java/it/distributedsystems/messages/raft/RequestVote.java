package it.distributedsystems.messages.raft;

import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.MessageDeserializerType;

public class RequestVote extends BaseDeserializableMessage {
    private final int candidateTerm;
    private final int candidateLastIndex;
    private final int candidateLastTerm;

    public RequestVote(int candidateTerm, int candidateLastIndex, int candidateLastTerm) {
        super(MessageDeserializerType.REQUEST_VOTE);
        this.candidateTerm = candidateTerm;
        this.candidateLastIndex = candidateLastIndex;
        this.candidateLastTerm = candidateLastTerm;
    }

    public int getCandidateTerm() {
        return candidateTerm;
    }

    public int getCandidateLastIndex() {
        return candidateLastIndex;
    }

    public int getCandidateLastTerm() {
        return candidateLastTerm;
    }
}
