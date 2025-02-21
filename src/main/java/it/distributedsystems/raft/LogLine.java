package it.distributedsystems.raft;

import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.queue.QueueCommand;

/**
 * This class represent one line of the Log File. It keeps track of the log index
 */
public class LogLine {
    private static int lastInsertedIndex = -1;

    private final int index;
    private final int epoch;
    private final QueueCommand command;

    /**
     * Constructor used to clone a LogLine, does not increase the index count
     */
    public LogLine(String fullFormattedLogLine) {
        var parts = fullFormattedLogLine.trim().split(";");
        this.index = Integer.parseInt(parts[0]);
        this.epoch = Integer.parseInt(parts[1]);
        this.command = (QueueCommand) GsonDeserializer.deserialize(parts[2]);
    }

    /**
     * Constructor used to clone a LogLine, does not increase the index count
     */
    public LogLine(int epoch, int index, QueueCommand command) {
        this.index = index;
        this.epoch = epoch;
        this.command = command;
    }

    /**
     * Constructor used to clone a LogLine, does not increase the index count
     */
    public LogLine(int epoch, int index, String commandJson) {
        this.index = index;
        this.epoch = epoch;
        this.command = (QueueCommand) GsonDeserializer.deserialize(commandJson);
    }

    /**
     * Static constructor to create a NEW LOGLINE.
     * Increase the index counter!
     */
    public static LogLine CreateNewLogToAppend(int epoch, QueueCommand command) {
        return new LogLine(++lastInsertedIndex, epoch, command);
    }

    /**
     * The to string method return the fully formatted line to be appended on LogFile
     * {INDEX};{EPOCH};{Json representation of QueueCommand}
     */
    @Override
    public String toString(){
        return String.format("%d;%d;%s", index, epoch, command.toJson());
    }

    //Getters

    /**
     * The returned index is the last inserted index in the LogFile.
     */
    public static int getLastInsertedIndex() {
        return lastInsertedIndex;
    }

    public int getIndex() {
        return index;
    }

    public int getEpoch() {
        return epoch;
    }

    public QueueCommand getCommand() {
        return command;
    }
}
