package it.distributedsystems.raft;

import it.distributedsystems.messages.queue.QueueCommand;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;

import static java.lang.System.exit;

/**
 * This class handles write of the log.
 * The logic is simple. The whole application has a base directory (hardcoded). Every Broker creates in this directory a directory with the date and then inside one file txt with the log.
 * -\homedir
 *   -\{date}
 *    -brokerID.txt
 *    -brokerID.txt
 *    -...
 *    Log structure: as a csv. Every log is in a new line.
 *    Index;Epoch;Json representation of QueueCommand
 */
public class ReplicationLog {
    public final static String FILE_HEADER = "Index;Epoch;JsonQueueCommand";
    private static String FILE_PATH = System.getProperty("user.dir") + "/logs/" + LocalDate.now();
    private static LogLine prevLogLine = null;

    /**
     * Method to be called at the initialization of the BrokerModel to initialize the log file.
     * Creates the file and add the header. Checks that the file does not exist
     */
    public static void initializeLogFile() {
        try {
            Files.createDirectories(Paths.get(FILE_PATH));
            FILE_PATH = FILE_PATH + "/" + BrokerSettings.getBrokerID() + ".txt";
            File file = new File(FILE_PATH);

            if (file.exists()) {
                //file already exist;
                //Maybe a crash, Recompute all replication log until the last saved leader commit indexn
                return;
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_PATH))) {
                writer.write(FILE_HEADER);
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }

    /**
     * Append the command to the file in the standard directory in brokerid.txt file.
     * @param command the new command to append
     * @return the appended LogLine
     */
    public static LogLine leaderAppendCommand(QueueCommand command) {
        //The Index count is incremented. Kept inside LogLine.getLastIndexCounter
        LogLine line = LogLine.CreateNewLogToAppend(BrokerSettings.getBrokerEpoch(), command);

        // Append the line to the file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_PATH, true))) {
            writer.write(line.toString());
            writer.newLine();
            //System.out.println("Appended line: " + line);
        } catch (IOException e) {
            System.err.println("Error while appending to file: " + e.getMessage());
            exit(0);
        }

        return line;
    }

    /**
     * Reads all the contents of the CSV file and apply the command to the BrokerModel.
     * Needed in case of reconnection after a crash. New Broker Model is instantiated and all the changes in the log applied
     * To be done AFTER LOG RECONCILIATION with (possibly new) Leader.
     * Recompute ALL until the LEADERCOMMITINDEX, the last committed entry
     */
    public static void recomputeAllReplicationLog() {
        File file = new File(FILE_PATH);
        if (!file.exists()) {
            System.out.println("File does not exist: " + FILE_PATH);
            return;
        }

        var lastLederCommitIndex = BrokerSettings.getBrokerCommitIndex();

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String logLineString;
            reader.readLine(); //skip header line

            BrokerModel.getInstance().acquireLock();
            while ((logLineString = reader.readLine()) != null) {
                var tmp = new LogLine(logLineString);
                if (tmp.getIndex() > lastLederCommitIndex) break;
                BrokerModel.getInstance().processCommand(tmp.getCommand());
            }

            BrokerModel.getInstance().releaseLock();
        } catch (InvalidObjectException e) {
            System.err.println("Error while extracting the command: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error while reading file: " + e.getMessage());
        }
    }

    public static LogLine getPrevLogLineString() {
        return prevLogLine;
    }

    public static int getPrevLogLineIndex() {
        return prevLogLine != null ? prevLogLine.getIndex() : -1;
    }

    public static int getPrevLogLineTerm() {
        return prevLogLine != null ? prevLogLine.getEpoch() : -1;
    }

    /**
     * Used passing the last entry of AppendEntries (received or sended)
     */
    public static void setPrevLogLine(String prevLogLineString) {
        ReplicationLog.prevLogLine = new LogLine(prevLogLineString);
    }

    public static void setPersistentState(){
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_PATH, true))) {
            writer.write(String.format(FILE_HEADER + "currentTerm=%d;votedFor=%d", BrokerSettings.getBrokerEpoch(), BrokerSettings.getCurrentTermVotedFor()));
            writer.newLine();
            //System.out.println("Appended line: " + line);
        } catch (IOException e) {
            System.err.println("Error while appending to file: " + e.getMessage());
            exit(0);
        }
    }
}
