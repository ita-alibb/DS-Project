package it.distributedsystems.raft;

import it.distributedsystems.messages.queue.CommandType;
import it.distributedsystems.messages.queue.QueueCommand;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class represent one line of the Log File. It keeps track of the log index
 */
public class LogLine {
    private static int lastInsertedIndex = -1;

    private final int index;
    private final int epoch;
    private final String commandLog;
    private final QueueCommand command;

    /**
     * Constructor used to clone a LogLine, does not increase the index count
     */
    public LogLine(String fullFormattedLogLine) {
        var parts = fullFormattedLogLine.trim().split(";");
        this.index = Integer.parseInt(parts[0]);
        this.epoch = Integer.parseInt(parts[1]);
        this.commandLog = parts[2]+";"+parts[3];

        this.command = extractCommand();
    }

    /**
     * Constructor used to clone a LogLine, does not increase the index count
     */
    public LogLine(int epoch, int index, QueueCommand command) {
        this.index = index;
        this.epoch = epoch;
        this.command = command;

        this.commandLog = formatCommand();
    }

    /**
     * Constructor used to clone a LogLine, does not increase the index count
     */
    public LogLine(int epoch, int index, String commandLog) {
        this.index = index;
        this.epoch = epoch;
        this.commandLog = commandLog;
        this.command = extractCommand();
    }

    /**
     * Static constructor to create a NEW LOGLINE.
     * Increase the index counter!
     */
    public static LogLine CreateNewLogToAppend(int epoch, QueueCommand command) {
        return new LogLine(++lastInsertedIndex, epoch, command);
    }

    /**
     * From commandLog to QueueCommand Object
     */
    private QueueCommand extractCommand() {
        // Regex pattern for all three formats
        String regex = "(.+);(CREATE_QUEUE|APPEND_DATA|READ_DATA)\\(([^,]*)(?:,([^,]*))?\\)";

        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(this.commandLog);

        if (matcher.matches()) {
            String senderID = matcher.group(1);  // First %s (SenderID)
            String command = matcher.group(2);    // Command type
            String queueKey = matcher.group(3);     // First parameter inside (queueKey)
            String data = matcher.group(4);     // Second parameter (if exists, data)

            CommandType type;
            switch (command) {
                case "CREATE_QUEUE" : type = CommandType.CREATE_QUEUE; break;
                case "APPEND_DATA" : type = CommandType.APPEND_DATA; break;
                case "READ_DATA" : type = CommandType.READ_DATA; break;
                default : {
                    System.out.println("Error: unknown command: " + this.commandLog);
                    return null;
                }
            };

            //TODO: al momento parseInt non e' safe, io farei una versione safe che restituisce null in caso non sia parsable
            return new QueueCommand(Integer.parseInt(senderID), type, queueKey, Integer.parseInt(data.strip()));
        } else {
            System.out.println("Error: cannot parse command: " + this.commandLog);
            return null;
        }
    }

    /**
     * From Queue Command Object to the formatted line that is appended in the Log (commandLog)!
     * "{clientID};{MethodName}{(params...)}"
     */
    public String formatCommand() {
        return switch (command.getType()) {
            case CREATE_QUEUE -> String.format("%s;CREATE_QUEUE(%s)", command.getClientID(), command.getQueueKey());
            case APPEND_DATA -> String.format("%s;APPEND_DATA(%s,%d)", command.getClientID(), command.getQueueKey(), command.getData());
            case READ_DATA -> String.format("%s;READ_DATA(%s)", command.getClientID(), command.getQueueKey());
        };
    }

    /**
     * The to string method return the fully formatted line to be appended on LogFile
     * {INDEX};{EPOCH};{clientID};{MethodName}({params...})
     */
    @Override
    public String toString(){
        return String.format("%d;%d;%s", index, epoch, commandLog);
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

    public String getCommandLog() {
        return commandLog;
    }

    public QueueCommand getCommand() {
        return command;
    }
}
