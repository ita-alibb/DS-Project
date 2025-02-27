package it.distributedsystems.raft;

import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.queue.QueueCommand;
import it.distributedsystems.messages.raft.AppendEntries;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

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
 *    -brokerID.csv
 *    -brokerID.csv
 *    -...
 *    Log structure: as a csv. Every log is in a new line.
 *    Index;Epoch;Json representation of QueueCommand
 */
public class ReplicationLog {
    //region File handling:
    public final static String FILE_HEADER = "Index;Epoch;JsonQueueCommand";
    private static String LOG_FILE_PATH = System.getProperty("user.dir") + "/logs/" + LocalDate.now()+ "/";
    private static String STATE_FILE_PATH;
    private final static char delimiter = ';';
    private static final CSVFormat csvFormat = CSVFormat.DEFAULT
            .builder()
            .setDelimiter(delimiter)
            .setHeader(FILE_HEADER)
            .setSkipHeaderRecord(true)
            .build();
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
            LOG_FILE_PATH = dir + BrokerSettings.getBrokerID() + ".csv";
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
     * Append the command to the file in the standard directory in brokerid.csv file.
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
        //Write to persistent file
        writeLineToCSV(line);

        return line;
    }

    /**
     * Called after check that prevIndex exists.
     * Append the logLine to the file in the standard directory in brokerid.csv file.
     * Used by follower to copy message from AppendEntries
     */
    public static void followerLogReconciliation(AppendEntries appendEntries) {
        //Here the logLine with prevLogIndex is sure to exist
        truncateLogAfterIndex(appendEntries.getPrevLogIndex());

        //Append all logLine
        LogLine line = ReplicationLog.lastLogLine;
        for (String logLine : appendEntries.getLogLineStringBatch()) {
            line = new LogLine(logLine, true);
            writeLineToCSV(line);
            appendToCachedLogLine(line);
        }

        //Set the new lastLogLine
        ReplicationLog.lastLogLine = line;
    }

    private static void writeLineToCSV(LogLine line) {
        try (FileWriter writer = new FileWriter(LOG_FILE_PATH, true);
             CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat)) {

            csvPrinter.printRecord((LogLine) line);
            csvPrinter.flush();
        } catch (IOException e) {
            System.err.println("Error while appending to file: " + e.getMessage());
            exit(0);
        }
    }
    /**
     * Reads all the contents of the CSV file and apply the command to the BrokerModel.
     * used when the commit index changes. Ensures that the lastApplied is in match with the commit index!
     */
    public synchronized static void applyReplicationLog() {
        var lastLeaderCommitIndex = BrokerState.getCommitIndex();
        var lastAppliedIndex = BrokerState.getLastApplied();

        if (lastAppliedIndex >= lastLeaderCommitIndex) return;

        try (Reader reader = new FileReader(LOG_FILE_PATH);
             CSVParser csvParser = new CSVParser(reader, csvFormat)) {

            BrokerModel.getInstance().acquireLock();

            for (CSVRecord record : csvParser) {
                //check index
                if (Integer.parseInt(record.get(0)) <= lastAppliedIndex) continue;
                if (Integer.parseInt(record.get(0)) > lastLeaderCommitIndex) break;

                //it's  lastApplied<=tmp.Index<=leaderCommitIndex --> Commit
                BrokerModel.getInstance().processCommand((QueueCommand) GsonDeserializer.deserialize(record.get(3)));
                BrokerState.setLastApplied(Integer.parseInt(record.get(0)));
            }
        } catch (FileNotFoundException e) {
            System.out.println("File does not exist: " + LOG_FILE_PATH);
            exit(-1);
        } catch (IOException e) {
            System.err.println("Error while reading file: " + e.getMessage());
            exit(-1);
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
        var cacheHit = cachedLogLines.stream().filter(ll -> ll.getIndex() == logIndex).toList();
        if (!cacheHit.isEmpty()) {
            return cacheHit.getLast();
        }

        //Cache miss:
        try (Reader reader = new FileReader(LOG_FILE_PATH);
             CSVParser csvParser = new CSVParser(reader, csvFormat)) {

            for (CSVRecord record : csvParser) {
                if (record.size() > 1 && Integer.parseInt(record.get(0)) == logIndex) {
                    return new LogLine(Integer.parseInt(record.get(0)), Integer.parseInt(record.get(1)), record.get(2));
                }
            }
        } catch (IOException e) {
            System.err.println("Error while reading file: " + e.getMessage());
            exit(-1);
        }
        return null;
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
        if (!cachedLogLines.offer(logLine)){
            cachedLogLines.poll();
            cachedLogLines.offer(logLine);
        }
    }
}