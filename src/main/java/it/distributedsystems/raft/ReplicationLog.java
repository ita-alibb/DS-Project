package it.distributedsystems.raft;

import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.queue.QueueCommand;
import it.distributedsystems.messages.raft.AppendEntries;
import it.distributedsystems.messages.raft.PastClientInfos;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.ReversedLinesFileReader;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
            .setHeader(FILE_HEADER.split(String.valueOf(delimiter)))
            .setSkipHeaderRecord(true)
            .build();
    //endregion

    //region pervLogLine : lastLogLine appended to file and cache
    private static LogLine lastLogLine = null; //The LAST line in your LOG!
    //private static LogLine prevLogLine = null; //The log line sent by the leader in appendEntries!
    private final static int CACHE_SIZE = 100;
    private static final LinkedList<LogLine> cachedLogLines = new LinkedList<>();
    private static final ReentrantReadWriteLock logUpdateLock = new ReentrantReadWriteLock();
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
                lastLogLine = getLastLogLineOnStartup();
                return;
            }

            // Create the file with just the header
            try (FileWriter fileWriter = new FileWriter(file);
                 CSVPrinter csvPrinter = new CSVPrinter(fileWriter, csvFormat)) {
                // No need to manually write the header - the CSVPrinter will do it automatically
                // because we set the header in the CSVFormat
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
            /*while ((line = reader.readLine()) != null && !line.equals(FILE_HEADER)) {
                try (CSVParser parser = CSVParser.parse(line, csvFormat)) {
                    for (CSVRecord record : parser) {
                        //read all records backward
                    }
                }
            }*/
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
        LogLine line = LogLine.CreateNewLogToAppend(BrokerState.getCurrentTerm(), command);

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
        try (FileWriter writer = new FileWriter(LOG_FILE_PATH, true);
             CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat)) {

            csvPrinter.printRecord(line);
            csvPrinter.flush();
            appendToCachedLogLine(line);
        } catch (IOException e) {
            System.err.println("Error while appending to file: " + e.getMessage());
            exit(0);
        } finally {
            logUpdateLock.writeLock().unlock();
        }
    }

    private static void writeLineToCSV(List<LogLine> line) {
        logUpdateLock.writeLock().lock();
        try (FileWriter writer = new FileWriter(LOG_FILE_PATH, true);
             CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat)) {

            for (LogLine logLine : line) {
                csvPrinter.printRecord(logLine);
                csvPrinter.flush();
                appendToCachedLogLine(logLine);
            }
        } catch (IOException e) {
            System.err.println("Error while appending to file: " + e.getMessage());
            exit(0);
        } finally {
            logUpdateLock.writeLock().unlock();
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

        logUpdateLock.readLock().lock();
        try (Reader reader = new FileReader(LOG_FILE_PATH);
             CSVParser csvParser = new CSVParser(reader, csvFormat)) {

            BrokerModel.getInstance().acquireLock();

            for (CSVRecord record : csvParser) {
                //check index
                if (Integer.parseInt(record.get(0)) <= lastAppliedIndex) continue;
                if (Integer.parseInt(record.get(0)) > lastLeaderCommitIndex) break;

                //it's  lastApplied<=tmp.Index<=leaderCommitIndex --> Commit
                BrokerModel.getInstance().processCommand((QueueCommand) GsonDeserializer.deserialize(record.get(2)));
                BrokerState.setLastApplied(Integer.parseInt(record.get(0)));
            }
        } catch (FileNotFoundException e) {
            System.out.println("File does not exist: " + LOG_FILE_PATH);
            exit(-1);
        } catch (IOException e) {
            System.err.println("Error while reading file: " + e.getMessage());
            exit(-1);
        } finally {
            logUpdateLock.readLock().unlock();
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
        } finally {
            logUpdateLock.readLock().unlock();
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
        //try using cache:
        var searchingInts = IntStream.rangeClosed(startIndex, lastLogLine.getIndex()).boxed().toList();
        var cacheHit = cachedLogLines.stream().filter(ll -> searchingInts.contains(ll.getIndex()))
                .sorted(Comparator.comparingInt(LogLine::getIndex)).toList();
        if (!cacheHit.isEmpty() && cacheHit.size() == searchingInts.size()) {//found all
            return cacheHit;
        }

        List<LogLine> logs = new ArrayList<>();

        //Cache miss:
        try (Reader reader = new FileReader(LOG_FILE_PATH);
             CSVParser csvParser = new CSVParser(reader, csvFormat)) {

            for (CSVRecord record : csvParser) {
                if (record.size() > 1 && Integer.parseInt(record.get(0)) >= startIndex) {
                    logs.add(new LogLine(Integer.parseInt(record.get(0)), Integer.parseInt(record.get(1)), record.get(2)));
                }
            }
        } catch (IOException e) {
            System.err.println("Error while reading file: " + e.getMessage());
            exit(-1);
        } finally {
            logUpdateLock.readLock().unlock();
        }

        return logs;
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

    /**
     * Cold start from log
     */
    public static HashMap<Integer, PastClientInfos> getPastClientsInfos() {
        logUpdateLock.readLock().lock();
        var returnMap = new HashMap<Integer, PastClientInfos>();
        try (Reader reader = new FileReader(LOG_FILE_PATH);
             CSVParser csvParser = new CSVParser(reader, csvFormat)) {
            for (CSVRecord record : csvParser) {
                var queueCommand = (QueueCommand) GsonDeserializer.deserialize(record.get(2));
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
        }
        return returnMap;
    }

    public static void cleanCache(){
        cachedLogLines.clear();
    }
}