package it.distributedsystems.raft;

import it.distributedsystems.messages.queue.QueueCommand;
import it.distributedsystems.messages.raft.AppendEntries;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.LinkedList;

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
    //region File handling:
    public final static String FILE_HEADER = "Index;Epoch;JsonQueueCommand";
    private static String LOG_FILE_PATH = System.getProperty("user.dir") + "/logs/" + LocalDate.now()+ "/";
    private static String STATE_FILE_PATH;
    //endregion

    //region pervLogLine : lastLogLine appended to file and cache
    private static LogLine lastLogLine = null; //The LAST line in your LOG!
    //private static LogLine prevLogLine = null; //The log line sent by the leader in appendEntries!
    private final static int CACHE_SIZE = 100;
    private static LinkedList<LogLine> cachedLogLines = new LinkedList<>();
    //endregion

    /**
     * Method to be called at the initialization of the BrokerModel to initialize the log file.
     * Creates the file and add the header. Checks that the file does not exist
     */
    public static void initializeLogFile() {
        try {
            Files.createDirectories(Paths.get(LOG_FILE_PATH));
            var dir = LOG_FILE_PATH;
            LOG_FILE_PATH = dir + BrokerSettings.getBrokerID() + ".txt";
            STATE_FILE_PATH = dir + BrokerSettings.getBrokerID() + "state" + ".txt";
            File file = new File(LOG_FILE_PATH);

            if (file.exists()) {
                //file already exist;
                //Maybe a crash, Recompute all replication log until the last saved leader commit indexn
                return;
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE_PATH, false))) {
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
        LogLine line = LogLine.CreateNewLogToAppend(BrokerState.getCurrentTerm(), command);

        //Append to the cache
        appendToCachedLogLine(line);
        //set as LastLog Line
        ReplicationLog.lastLogLine = line;

        // Append the line to the file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE_PATH, true))) {
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
     * Append the logLine to the file in the standard directory in brokerid.txt file.
     * Used by follower to copy message from AppendEntries
     */
    public static void followerCopyAppendEntriesLog(AppendEntries appendEntries) {
        //TODO: copia tutti i log, a partire dal prev che deve matchare fino all'ultimo.
        // attenzione che in certi casi il prev matcha, ma non con il mio ultimo!
        //ReplicationLog.lastLogLine = line;
    }

    /**
     * Reads all the contents of the CSV file and apply the command to the BrokerModel.
     * used when the commit index changes. Ensures that the lastApplied is in match with the commit index!
     */
    public static void applyReplicationLog() {
        File file = new File(LOG_FILE_PATH);
        if (!file.exists()) {
            System.out.println("File does not exist: " + LOG_FILE_PATH);
            return;
        }

        var lastLeaderCommitIndex = BrokerState.getCommitIndex();
        var lastAppliedIndex = BrokerState.getLastApplied();

        if (lastAppliedIndex >= lastLeaderCommitIndex) return;

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String logLineString;
            reader.readLine(); //skip header line

            //lock the model
            BrokerModel.getInstance().acquireLock();

            while ((logLineString = reader.readLine()) != null) {
                var tmp = new LogLine(logLineString);
                if (tmp.getIndex() <= lastAppliedIndex) continue;
                if (tmp.getIndex() > lastLeaderCommitIndex) break;

                //it's  lastApplied<=tmp.Index<=leaderCommitIndex --> Commit
                BrokerModel.getInstance().processCommand(tmp.getCommand());
                BrokerState.setLastApplied(tmp.getIndex());
            }

            //unlock the model
            BrokerModel.getInstance().releaseLock();
        } catch (InvalidObjectException e) {
            System.err.println("Error while extracting the command: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error while reading file: " + e.getMessage());
        }
    }
    public static int getPrevLogLineIndex() {
        return lastLogLine != null ? lastLogLine.getIndex() : -1;
    }

    public static int getPrevLogLineTerm() {
        return lastLogLine != null ? lastLogLine.getTerm() : -1;
    }

    public synchronized static void storePersistentState(String persistentState){
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(STATE_FILE_PATH, false))) {
            writer.write(String.format(FILE_HEADER + persistentState));
            writer.newLine();
            //System.out.println("Appended line: " + line);
        } catch (IOException e) {
            System.err.println("Error while appending to file: " + e.getMessage());
            exit(0);
        }
    }

    /**
     * Get the log with specified index
     */
    public static LogLine getLog(int logIndex) {
        //try using cache:
        var cacheHit = cachedLogLines.stream().filter(ll -> ll.getIndex() == logIndex).findFirst();
        if (cacheHit.isPresent()) {
            return cacheHit.get();
        }

        //Cache miss:
        File file = new File(LOG_FILE_PATH);
        if (!file.exists()) {
            exit(-1);
            return null;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String logLineString;
            reader.readLine(); //skip header line

            LogLine hit = null;
            while ((logLineString = reader.readLine()) != null) {
                hit = new LogLine(logLineString);
                if (hit.getIndex() > logIndex) break;
            }

            return hit;
        } catch (InvalidObjectException e) {
            System.err.println("Error while extracting the command: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error while reading file: " + e.getMessage());
        }

        return null;
    }

    private static void appendToCachedLogLine(LogLine logLine) {
        if (!cachedLogLines.offer(logLine)){
            cachedLogLines.poll();
            cachedLogLines.offer(logLine);
        }
    }
}
