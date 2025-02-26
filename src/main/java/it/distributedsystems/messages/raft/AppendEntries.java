package it.distributedsystems.messages.raft;

import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.MessageDeserializerType;
import it.distributedsystems.raft.LogLine;

import java.util.*;

/**
 * AppendEntries message
 *
 *  Invoked by leader to replicate log entries (§5.3); also used as
 *  heartbeat(§5.2).<br>
 *  Arguments:<br>
 *  term leader’s term
 *  leaderId so follower can redirect clients<br>
 *  prevLogIndex index of log entry immediately preceding
 *  new ones<br>
 *  prevLogTerm term of prevLogIndex entry<br>
 *  entries[] log entries to store(empty for heartbeat;
 *  may send more than one for efficiency)<br>
 *  leaderCommit leader’s commitIndex<br>
 *  Results:<br>
 *  term currentTerm, for leader to update itself<br>
 *  success true if follower contained entry matching
 *  prevLogIndex and prevLogTerm<br>
 *  Receiver implementation:<br>
 *  1. Reply false if term< currentTerm(§5.1)<br>
 *  2. Reply false if log doesn’t contain an entry at prevLogIndex
 *  whose term matches prevLogTerm(§5.3)<br>
 *  3. If an existing entry conflicts with a new one(same index
 *  but different terms),delete the existing entry and all that
 *  follow it(§5.3)<br>
 *  4. Append any new entries not already in the log<br>
 *  5. If leaderCommit>commitIndex,set commitIndex=
 *  min(leaderCommit,index of last new entry)<br>
 */
public class AppendEntries extends BaseDeserializableMessage {
    // Raft AppendEntries
    private final int leaderTerm;
    private final int leaderID;
    private int prevLogIndex;
    private int prevLogTerm;
    /**
     * List of append entries, contains LogLineString
     */
    private final List<String> logLineBatch;
    /**
     * Transient property to operate with Obj when creating the batch
     */
    private transient final List<LogLine> logLineObjBatch;
    private int leaderCommitIndex;

    public AppendEntries(int leaderTerm, int leaderID) {
        super(MessageDeserializerType.APPEND_ENTRIES);
        this.leaderTerm = leaderTerm;
        this.leaderID = leaderID;
        this.prevLogIndex = 0;
        this.prevLogTerm = 0;
        this.logLineBatch = new ArrayList<>();
        this.logLineObjBatch = new ArrayList<>();
        this.leaderCommitIndex = -1;
    }

    /**
     * Constructor used to clone
     */
    public AppendEntries(AppendEntries currentBatchMessage) {
        super(MessageDeserializerType.APPEND_ENTRIES);
        this.leaderTerm = currentBatchMessage.leaderTerm;
        this.leaderID = currentBatchMessage.leaderID;
        this.prevLogIndex = currentBatchMessage.prevLogIndex;
        this.prevLogTerm = currentBatchMessage.prevLogTerm;
        this.logLineBatch = new ArrayList<>();
        this.logLineBatch.addAll(currentBatchMessage.logLineBatch);
        this.logLineObjBatch = new ArrayList<>();
        this.leaderCommitIndex = currentBatchMessage.leaderCommitIndex;
    }

    public int getLeaderTerm() {
        return leaderTerm;
    }

    public int getLeaderID() {
        return leaderID;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public LogLine getLastLogLineInBatch() {
        if (!logLineObjBatch.isEmpty()) {
            return logLineObjBatch.getLast();
        } else {
            return null;
        }
    }

    public int getLastNewLineIndex() {
        if (!logLineObjBatch.isEmpty()) {
            return logLineObjBatch.getLast().getIndex();
        } else if (!logLineBatch.isEmpty()) {
            var lastLogLine = new LogLine(logLineBatch.getLast());
            return lastLogLine.getIndex();
        }

        return -1;
    }

    public int getFirstNewLineIndex() {
        if (!logLineObjBatch.isEmpty()) {
            return logLineObjBatch.getLast().getIndex();
        } else if (!logLineBatch.isEmpty()) {
            var lastLogLine = new LogLine(logLineBatch.getFirst());
            return lastLogLine.getIndex();
        }

        return 0;
    }

    public int getLeaderCommitIndex() {
        return leaderCommitIndex;
    }

    public List<String> getLogLineStringBatch() {
        return new ArrayList<>(logLineBatch);
    }

    public void clearBatch(){
        this.logLineBatch.clear();
        this.logLineObjBatch.clear();
    }

    public void addNewLogLine(LogLine logLine){
        this.logLineBatch.add(logLine.toString().strip());
        this.logLineObjBatch.add(logLine);
    }

    public void setLeaderCommitIndex(int leaderCommitIndex) {
        this.leaderCommitIndex = leaderCommitIndex;
    }

    public void setPrevLogIndex(int prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }
}
