package it.distributedsystems.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import it.distributedsystems.raft.BrokerSettings;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static it.distributedsystems.raft.ReplicationLog.FILE_HEADER;
import static java.lang.System.exit;

/**
 * Class used to protect setters, called by the main app once at the start. Bootstrap of every final setting
 */
public class BrokerSettingsBootstrapper extends BrokerSettings {
    public static void bootstrap(int brokerId){
        final Path path = Path.of(System.getProperty("user.dir"),"brokerPortConfiguration.json");

        String jsonText = null;
        try {
            jsonText = Files.readString(path);
        } catch (IOException e) {
            System.out.println("Cannot find configuration file");
            exit(-1);
        }
        Gson gson = new Gson();

        List<BrokerAddress> newBrokerAddress = gson.fromJson(jsonText, new TypeToken<List<BrokerAddress>>() {}.getType());

        setBrokerAddress(newBrokerAddress.stream().filter(ba -> ba.id == brokerId).findFirst().get());
        //remove myself from the set of known brokers
        newBrokerAddress.removeIf(ba -> ba.id == brokerId);
        setBrokers(newBrokerAddress);
        setNumOfNodes(newBrokerAddress.size() + 1);

        restorePersistentState();
    }


    /**
     * Consider the header of the log file as the persistent state. It stores also "currentTerm=X;votedFor=Y"
     */
    private static void restorePersistentState(){
        File file = new File(System.getProperty("user.dir") + "/logs/" + LocalDate.now() + "/" + BrokerSettings.getBrokerID() + ".txt");
        if (!file.exists()) {
            System.out.println("File does not exist on restore");
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            var persistentState = reader.readLine().replace(FILE_HEADER,""); //remove header

            if (persistentState.isEmpty()) return;

            persistentState= persistentState.replace("currentTerm=","");
            persistentState= persistentState.replace("votedFor=","");

            var match = persistentState.split(";");
            BrokerSettings.setBrokerEpoch(Integer.parseInt(match[0]));
            if (match.length > 1 && !match[1].isEmpty()) {
                BrokerSettings.setCurrentTermVotedFor(Integer.parseInt(match[1]));
            }
        } catch (InvalidObjectException e) {
            System.err.println("Error while extracting the command: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error while reading file: " + e.getMessage());
        }
    }
}
