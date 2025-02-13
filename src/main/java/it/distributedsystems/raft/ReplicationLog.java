package it.distributedsystems.raft;

import it.distributedsystems.commands.*;

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
 *    Epoch;SenderID;Command
 *
 */
public class ReplicationLog {
    private static final String FILE_PATH = System.getProperty("user.home") + "/Desktop/DS-Project/" + LocalDate.now();

    /**
     * Method to be called at the initialization of the BrokerModel to initialize the log file.
     * Creates the file and add the header. Checks that the file does not exist
     */
    public static void initializeLogFile(String brokerID) {
        File file = new File(FILE_PATH + "/" + brokerID);

        if (file.exists()) {
            //file already exist; return
            return;
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_PATH + "/" + brokerID))) {
            writer.write("Epoch;SenderID;Command");
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
    }

    /**
     * Append the command to the file in the standard directory in brokerid.txt file.
     * @param brokerID the broker id, will be the name of the file
     * @param command the new command to append
     */
    public static void appendCommandInFile(String brokerID, int epoch, ClientCommand command) {
        String line = switch (command.getType()) {
            case CREATE_QUEUE -> String.format("%s;%s;CREATE_QUEUE(%s)", epoch, command.getSenderID(), command.getQueueKey());
            case APPEND_DATA -> String.format("%s;%s;APPEND_DATA(%s,%s)", epoch, command.getSenderID(), command.getQueueKey(), command.getData());
            case READ_DATA -> String.format("%s;%s;READ_DATA(%s)", epoch, command.getSenderID(), command.getQueueKey());
        };

        // Append the line to the file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_PATH + "/" + brokerID, true))) {
            writer.write(line);
            writer.newLine();
            //System.out.println("Appended line: " + line);
        } catch (IOException e) {
            //System.err.println("Error while appending to file: " + e.getMessage());
        }
    }

    // Reads all the contents of the CSV file and apply the command
    public static void recomputeAllReplicationLog(String brokerID) {
        File file = new File(FILE_PATH + "/" + brokerID);
        if (!file.exists()) {
            System.out.println("File does not exist: " + FILE_PATH);
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            reader.readLine(); //skip header line

            BrokerModel.getInstance().acquireLock();
            while ((line = reader.readLine()) != null) {
                BrokerModel.getInstance().processCommand(extractCommand(line));
            }
            BrokerModel.getInstance().releaseLock();
        } catch (InvalidObjectException e) {
            System.err.println("Error while extracting the command: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error while reading file: " + e.getMessage());
        }
    }

    private static ClientCommand extractCommand(String formattedLine) throws InvalidObjectException {
        // Regex pattern for all three formats
        String regex = "(.+);(.+);(CREATE_QUEUE|APPEND_DATA|READ_DATA)\\(([^,]*)(?:,([^,]*))?\\)";

        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(formattedLine);

        if (matcher.matches()) {
            String epoch = matcher.group(1);  // First %s (Epoch)
            String senderID = matcher.group(2);  // Second %s (SenderID)
            String command = matcher.group(3);    // Command type
            String queueKey = matcher.group(4);     // First parameter inside (queueKey)
            String data = matcher.group(5);     // Second parameter (if exists, data)

            CommandType type = switch (command) {
                case "CREATE_QUEUE" -> CommandType.CREATE_QUEUE;
                case "APPEND_DATA" -> CommandType.APPEND_DATA;
                case "READ_DATA" -> CommandType.READ_DATA;
                default -> throw new InvalidObjectException("No match in command type");
            };

            return new ClientCommand(senderID, type, queueKey, Integer.parseInt(data.strip()));
        } else {
            throw new InvalidObjectException("No match for whole regex");
        }
    }
}
