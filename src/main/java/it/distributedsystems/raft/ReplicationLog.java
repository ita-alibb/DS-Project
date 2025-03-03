package it.distributedsystems.raft;

import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.queue.QueueCommand;
import it.distributedsystems.messages.raft.AppendEntries;
import it.distributedsystems.messages.raft.PastClientInfos;
import org.apache.commons.io.input.ReversedLinesFileReader;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;

import static it.distributedsystems.raft.BrokerSettings.APPEND_ENTRIES_TIME;
import static java.lang.System.exit;

/**
 * This class handles write of the log.
 * The logic is simple. The whole application has a base directory (hardcoded). Every Broker creates in this directory a directory with the date and then inside one file txt with the log.
 * -\homedir
 *   -\{date}
 *    -brokerID.csv
 *    -brokerID.csv
 *    -...
 *    Log structure: as a csv. Every log is in a new line.
 *    Index;Epoch;Json representation of QueueCommand
 */
public class ReplicationLog {
    //region File handling:
    public final static String FILE_HEADER = "Index;Epoch;JsonQueueCommand";
    protected static String LOG_FILE_PATH = System.getProperty("user.dir") + "/logs/" + LocalDate.now()+ "/";
    protected static String STATE_FILE_PATH;
    private final static char delimiter = ';';
    //endregion

    //region pervLogLine : lastLogLine appended to file and cache
    private static LogLine lastLogLine = null; //The LAST line in your LOG!
    //private static LogLine prevLogLine = null; //The log line sent by the leader in appendEntries!
    private final static int CACHE_SIZE = 100;
    private static final LinkedList<LogLine> cachedLogLines = new LinkedList<>();
    private static final ReentrantReadWriteLock logUpdateLock = new ReentrantReadWriteLock(true);
    //endregion

    /**
     * Method to be called at the initialization of the BrokerModel to initialize the log file.
     * Creates the file and add the header. Checks that the file does not exist
     */
    public static void initializeLogFile() {
        try {
            Files.createDirectories(Paths.get(LOG_FILE_PATH));
            var dir = LOG_FILE_PATH;
            LOG_FILE_PATH = dir + BrokerSettings.getBrokerID() + ".csv";
            STATE_FILE_PATH = dir + BrokerSettings.getBrokerID() + "state" + ".txt";
            File file = new File(LOG_FILE_PATH);

            if (file.exists()) {
                //file already exist;
                //Maybe a crash, Recompute all replication log until the last saved leader commit index
                lastLogLine = getLastLogLineOnStartup();
                return;
            }

            // Create the file with just the header
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, false))){
                writer.write(FILE_HEADER);
                writer.newLine();
                writer.flush();
            }
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }

    /**
     * Return the last log line
     */
    private static LogLine getLastLogLineOnStartup(){
        File file = new File(LOG_FILE_PATH);

        try (ReversedLinesFileReader reader = new ReversedLinesFileReader(file, StandardCharsets.UTF_8)) {
            String line = reader.readLine();
            if  (line != null && !line.equals(FILE_HEADER)) {
                return new LogLine(line);
            }
        } catch (Exception e) {
            System.out.println("Exception while reading file backward");
            exit(-1);
        }

        return null;
    }

    /**
     * Append the command to the file in the standard directory in brokerid.csv file.
     * @param command the new command to append
     * @return the appended LogLine
     */
    public static LogLine leaderAppendCommand(QueueCommand command) {
        //The Index count is incremented. Kept inside LogLine.getLastIndexCounter
        LogLine line = LogLine.CreateNewLogToAppend(command);

        //Write to persistent file, populate also cache
        writeLineToCSV(line);
        //set as LastLog Line
        ReplicationLog.lastLogLine = line;

        return line;
    }

    /**
     * Called after check that prevIndex exists.
     * Append the logLine to the file in the standard directory in brokerid.csv file.
     * Used by follower to copy message from AppendEntries
     */
    public static void followerLogReconciliation(AppendEntries appendEntries) {
        //if there is no entry there is nothing to reconciliate
        if (appendEntries.getLogLineStringBatch().isEmpty()) return;

        //Here the logLine with prevLogIndex is sure to exist
        truncateLogAfterIndex(appendEntries.getPrevLogIndex());

        //Append all logLine
        List<LogLine> lines = appendEntries.getLogLineStringBatch().stream().map(s -> new LogLine(s,true)).toList();
        writeLineToCSV(lines);

        //Set the new lastLogLine
        ReplicationLog.lastLogLine = lines.getLast();
    }

    private static void writeLineToCSV(LogLine line) {
        logUpdateLock.writeLock().lock();
        System.err.println("LOCK DEBUG: writeLineToCSV single acquired the WRITE lock");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE_PATH, true))) {
            writer.write(line.toString());
            System.out.println("Written to file " + line.toString());
            writer.newLine();

            appendToCachedLogLine(line);
        } catch (IOException e) {
            System.err.println("Error while appending to file: " + e.getMessage());
            exit(0);
        } finally {
            System.out.println("Finished writing to file");
            logUpdateLock.writeLock().unlock();
            System.err.println("LOCK DEBUG: writeLineToCSV single released the WRITE lock");
        }
    }

    private static void writeLineToCSV(List<LogLine> line) {
        logUpdateLock.writeLock().lock();
        System.err.println("LOCK DEBUG: writeLineToCSV List acquired the WRITE lock");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE_PATH, true))) {
            for (LogLine logLine : line) {
                System.out.println("Written to file " + line.toString());
                writer.write(logLine.toString());
                writer.newLine();

                appendToCachedLogLine(logLine);
            }
        } catch (IOException e) {
            System.err.println("Error while appending to file: " + e.getMessage());
            exit(0);
        } finally {
            logUpdateLock.writeLock().unlock();
            System.err.println("LOCK DEBUG: writeLineToCSV list released the WRITE lock");
        }
    }

    /**
     * Safe way to apply replication log in a separate thread
     */
    public static void applyReplicationLogAsync() {
        Thread applyThread = new Thread(() -> {
            try {
                applyReplicationLog();
            } catch (Exception e) {

            }
        });

        applyThread.setName("LogApplicator-" + System.currentTimeMillis());
        applyThread.start();
    }

    /**
     * Reads all the contents of the CSV file and apply the command to the BrokerModel.
     * used when the commit index changes. Ensures that the lastApplied is in match with the commit index!
     */
    private synchronized static void applyReplicationLog() {
        var lastLeaderCommitIndex = BrokerState.getCommitIndex();
        var lastAppliedIndex = BrokerState.getLastApplied();

        try{
            logUpdateLock.readLock().lock();
            System.err.println("LOCK DEBUG: applyReplicationLog acquired the READ lock");

            System.out.println("Applying Replicated log from " + lastAppliedIndex + " to " + lastLeaderCommitIndex);

            if (lastAppliedIndex >= lastLeaderCommitIndex) return;
            try (BufferedReader reader = new BufferedReader(new FileReader(LOG_FILE_PATH))) {
                reader.readLine(); //discard header

                // Track if we've acquired the broker model lock
                try {
                    BrokerModel.getInstance().acquireLock();

                    String line;
                    while ((line = reader.readLine()) != null && !line.equals(FILE_HEADER)) {
                        var logLine = new LogLine(line);
                        //check index
                        if (logLine.getIndex() <= lastAppliedIndex) continue;
                        if (logLine.getIndex() > lastLeaderCommitIndex) break;

                        //it's  lastApplied<=tmp.Index<=leaderCommitIndex --> Commit
                        BrokerModel.getInstance().processCommand(logLine.getCommand());
                        BrokerState.setLastApplied(logLine.getIndex());
                    }
                } catch (Exception e) {
                    System.out.println("Exception while applying line: " + e.getMessage());
                }finally {
                    BrokerModel.getInstance().releaseLock();
                }
            } catch (FileNotFoundException e) {
                System.out.println("File does not exist: " + LOG_FILE_PATH);
                exit(-1);
            } catch (IOException e) {
                System.err.println("Error while reading file: " + e.getMessage());
                exit(-1);
            }
        } finally {
            logUpdateLock.readLock().unlock();
            System.err.println("LOCK DEBUG: ApplyReplicationLog unlocked the READ logUpdateLock");
        }
    }

    public static int getLastLogLineIndex() {
        return lastLogLine != null ? lastLogLine.getIndex() : 0;
    }

    public static int getLastLogLineTerm() {
        return lastLogLine != null ? lastLogLine.getTerm() : 0;
    }

    public synchronized static void storePersistentState(String persistentState){
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(STATE_FILE_PATH, false))) {
            writer.write(String.format(persistentState));
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
        if (logIndex <= 0) { //indexes starts from 1
            return null;
        }

        logUpdateLock.readLock().lock();
        try{
            System.err.println("LOCK DEBUG: getLog acquired the READ lock");
            //try using cache:
            var cacheHit = cachedLogLines.stream().filter(ll -> ll.getIndex() == logIndex).toList();
            if (!cacheHit.isEmpty()) {
                return cacheHit.getLast();
            }

            //Cache miss:
            try (BufferedReader reader = new BufferedReader(new FileReader(LOG_FILE_PATH))) {
                reader.readLine(); //discard header

                String line;

                while ((line = reader.readLine()) != null && !line.equals(FILE_HEADER)) {
                    var logLine = new LogLine(line, true);

                    if (logLine.getIndex() == logIndex) {
                        return logLine;
                    }
                }
            } catch (IOException e) {
                System.err.println("Error while reading file: " + e.getMessage());
                exit(-1);
            }
        } finally {
            logUpdateLock.readLock().unlock();
            System.err.println("LOCK DEBUG: getLog released the READ lock");
        }
        return null;
    }

    /**
     * Get the log with specified index
     */
    public static List<LogLine> getLogsFromStartIndex(int startIndex) {
        if (startIndex < 0) { //indexes starts from 1
            return new ArrayList<>();
        }

        logUpdateLock.readLock().lock();
        try{
            System.err.println("LOCK DEBUG: getLogsFromStartIndex acquired the READ lock");

            //try using cache:
            var searchingInts = IntStream.rangeClosed(startIndex, lastLogLine.getIndex()).boxed().toList();
            var cacheHit = cachedLogLines.stream().filter(ll -> searchingInts.contains(ll.getIndex()))
                    .sorted(Comparator.comparingInt(LogLine::getIndex)).toList();
            if (!cacheHit.isEmpty() && cacheHit.size() == searchingInts.size()) {//found all
                return cacheHit;
            }

            List<LogLine> logs = new ArrayList<>();

            //Cache miss:
            //Start reading backwards. Add on start
            try (ReversedLinesFileReader reader = new ReversedLinesFileReader(new File(LOG_FILE_PATH), StandardCharsets.UTF_8)) {
                String line;

                while ((line = reader.readLine()) != null && !line.equals(FILE_HEADER)) {

                    if (Integer.parseInt(line.split(";")[0]) >= startIndex) {
                        logs.addFirst(new LogLine(line));
                    }
                }
            } catch (IOException e) {
                System.err.println("Error while reading file: " + e.getMessage());
                exit(-1);
            }

            return logs;
        } finally {
            logUpdateLock.readLock().unlock();
            System.err.println("LOCK DEBUG: getLogsFromStartIndex released the READ lock");
        }
    }

    /**
     * Efficiently truncates the file by keeping only the rows up to the specified index.
     * Used to reconciliate log, delete every log and then append the right ones.
     * @param prevLogIndex The last row index to keep (inclusive, 0-based)
     * @return true if successful, false otherwise
     */
    private static boolean truncateLogAfterIndex(int prevLogIndex) {
        if (prevLogIndex < 0 || prevLogIndex == getLastLogLineIndex()) {
            return false;
        }

        try (RandomAccessFile file = new RandomAccessFile(LOG_FILE_PATH, "rw")) {
            String line;
            while((line = file.readLine()) != null) {
                //keep reading until reaching the last line to keep
                if (Integer.parseInt(line.split(delimiter+"")[0]) == prevLogIndex) break;
            }

            //Truncate all next lines
            file.setLength(file.getFilePointer());
        } catch (IOException e) {
            System.out.println("Cannot truncate file at line " + prevLogIndex);
            exit(-1);
        }

        return true;
    }

    private static void appendToCachedLogLine(LogLine logLine) {
        if (cachedLogLines.size() > CACHE_SIZE){
            cachedLogLines.poll();
        }
        cachedLogLines.offer(logLine);
    }

    /**
     * Cold start from log
     */
    public static HashMap<Integer, PastClientInfos> getPastClientsInfos() {
        logUpdateLock.readLock().lock();
        System.err.println("LOCK DEBUG: getPastClientInfos acquired the READ lock");

        var returnMap = new HashMap<Integer, PastClientInfos>();
        try (BufferedReader reader = new BufferedReader(new FileReader(LOG_FILE_PATH))) {
            reader.readLine(); //discard header

            String line;

            while ((line = reader.readLine()) != null && !line.equals(FILE_HEADER)) {
                var queueCommand = (QueueCommand) GsonDeserializer.deserialize(line.split(";")[2]);

                returnMap.put(
                        queueCommand.getClientID(),
                        new PastClientInfos(queueCommand.getClientID(), queueCommand.getCommandID(), null)
                );
            }
        } catch (FileNotFoundException e) {
            System.out.println("File does not exist: " + LOG_FILE_PATH);
            exit(-1);
        } catch (IOException e) {
            System.err.println("Error while reading file: " + e.getMessage());
            exit(-1);
        } finally {
            logUpdateLock.readLock().unlock();
            System.err.println("LOCK DEBUG: getPastClientInfo released the READ lock");
        }
        return returnMap;
    }

    public static void cleanCache(){
        cachedLogLines.clear();
    }
}