package it.distributedsystems.messages.raft;

import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.MessageDeserializerType;
import it.distributedsystems.raft.LogLine;

import java.util.*;

/**
 * AppendEntries message
 */
public class AppendEntries extends BaseDeserializableMessage {
    // Raft AppendEntries
    private final int epoch;
    private final int leaderID;
    /**
     * The full formatted prev log line string
     */
    private String prevLogLineString;

    /**
     * List of append entries, contains LogLineString
     */
    private final List<String> newLogLineBatch;
    private int leaderCommitIndex;

    public AppendEntries(int epoch, int leaderID) {
        super(MessageDeserializerType.APPEND_ENTRIES);
        this.epoch = epoch;
        this.leaderID = leaderID;
        this.prevLogLineString = null;
        this.newLogLineBatch = new ArrayList<>();
        this.leaderCommitIndex = -1;
    }

    /**
     * Constructor used to clone
     */
    public AppendEntries(AppendEntries currentBatchMessage) {
        super(MessageDeserializerType.APPEND_ENTRIES);
        this.epoch = currentBatchMessage.epoch;
        this.leaderID = currentBatchMessage.leaderID;
        this.prevLogLineString = currentBatchMessage.prevLogLineString;
        this.newLogLineBatch = new ArrayList<>();
        this.newLogLineBatch.addAll(currentBatchMessage.newLogLineBatch);
        this.leaderCommitIndex = currentBatchMessage.leaderCommitIndex;
    }

    public int getEpoch() {
        return epoch;
    }

    public int getLeaderID() {
        return leaderID;
    }

    public String getPrevLogLineString() {
        return prevLogLineString;
    }

    public List<String> getNewLogLineBatch() {
        return newLogLineBatch;
    }

    public int getLeaderCommitIndex() {
        return leaderCommitIndex;
    }

    public void clearBatch(){
        this.newLogLineBatch.clear();
    }

    public void addNewLogLine(String formattedLogLine){
        this.newLogLineBatch.add(formattedLogLine.strip());
    }

    public void addNewLogLine(LogLine logLine){
        this.newLogLineBatch.add(logLine.toString().strip());
    }

    public void setPrevLogLineString(String prevLogLineString) {
        this.prevLogLineString = prevLogLineString;
    }

    public void setLeaderCommitIndex(int leaderCommitIndex) {
        this.leaderCommitIndex = leaderCommitIndex;
    }
}
