package it.distributedsystems.raft;

import it.distributedsystems.messages.queue.CommandType;
import it.distributedsystems.messages.queue.QueueCommand;

import java.io.*;
import java.util.regex.*;
import java.time.LocalDate;

/**
 * This class handles write of the log.
 * The logic is simple. The whole application has a base directory (hardcoded). Every Broker creates in this directory a directory with the date and then inside one file txt with the log.
 * -\homedir
 *   -\{date}
 *    -brokerID.txt
 *    -brokerID.txt
 *    -...
 *
 *
 *    Log structure: as a csv. Every log is in a new line.
 *    Index;Epoch;SenderID;Command
 *     TODO: it is important to have the index of the log (which is the line number, but either you find a good way to get it or you should count it by yourself) and maybe not make it part of the log?
 */
public class ReplicationLog {
    private static String FILE_PATH = System.getProperty("user.home") + "/Desktop/DS-Project/" + LocalDate.now();

    /**
     * Method to be called at the initialization of the BrokerModel to initialize the log file.
     * Creates the file and add the header. Checks that the file does not exist
     */
    public static void initializeLogFile() {
        FILE_PATH = FILE_PATH + "/" + BrokerSettings.getBrokerID() + ".txt";
        File file = new File(FILE_PATH);

        if (file.exists()) {
            //file already exist; return
            return;
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_PATH))) {
            writer.write("Index;Epoch;ClientID;FormattedCommand");
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }

    /**
     * Append the command to the file in the standard directory in brokerid.txt file.
     * @param command the new command to append
     */
    public static void leaderAppendCommand(int epoch, QueueCommand command) {
        //The Index count is incremented. Kept inside LogLine.getLastIndexCounter
        LogLine line = LogLine.CreateNewLogToAppend(epoch, command);

        // Append the line to the file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_PATH, true))) {
            writer.write(line.toString());
            writer.newLine();
            //System.out.println("Appended line: " + line);
        } catch (IOException e) {
            System.err.println("Error while appending to file: " + e.getMessage());
        }
    }

    /**
     * Reads all the contents of the CSV file and apply the command to the BrokerModel.
     * Needed in case of reconnection after a crash. New Broker Model is instantiated and all the changes in the log applied
     * To be done AFTER LOG RECONCILIATION with (possibly new) Leader
     */
    public static void recomputeAllReplicationLog() {
        File file = new File(FILE_PATH);
        if (!file.exists()) {
            System.out.println("File does not exist: " + FILE_PATH);
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String logLineString;
            reader.readLine(); //skip header line

            BrokerModel.getInstance().acquireLock();
            while ((logLineString = reader.readLine()) != null) {
                var tmp = new LogLine(logLineString);
                BrokerModel.getInstance().processCommand(tmp.getCommand());
            }

            BrokerModel.getInstance().releaseLock();
        } catch (InvalidObjectException e) {
            System.err.println("Error while extracting the command: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error while reading file: " + e.getMessage());
        }
    }
}
