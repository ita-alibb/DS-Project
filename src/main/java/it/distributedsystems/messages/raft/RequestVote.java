package it.distributedsystems.messages.raft;

import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.MessageDeserializerType;

/**
 *  Arguments:
 *  term candidate’s term
 *  candidateId candidate requesting vote
 *  lastLogIndex index of candidate’s last log entry
 *  lastLogTerm term of candidate’s last log entry
 *  Results:
 *  term currentTerm,for candidate to update itself
 *  voteGranted true means candidate received vote
 *  Receiver implementation:
 *  1. Reply false if term< currentTerm(§5.1)
 *  2. If votedFor is null or candidate Id,and candidate’s log is at
 *  least as up-to-date as receiver’s log,grant vote(§5.2,§5.4)
 */
public class RequestVote extends BaseDeserializableMessage {
    private final int candidateId;
    private final int candidateTerm;
    private final int candidateLastLogIndex;
    private final int candidateLastLogTerm;

    public RequestVote(int candidateId, int candidateTerm, int candidateLastIndex, int candidateLastTerm) {
        super(MessageDeserializerType.REQUEST_VOTE);
        this.candidateId = candidateId;
        this.candidateTerm = candidateTerm;
        this.candidateLastLogIndex = candidateLastIndex;
        this.candidateLastLogTerm = candidateLastTerm;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public int getCandidateTerm() {
        return candidateTerm;
    }

    public int getCandidateLastLogIndex() {
        return candidateLastLogIndex;
    }

    public int getCandidateLastLogTerm() {
        return candidateLastLogTerm;
    }
}
