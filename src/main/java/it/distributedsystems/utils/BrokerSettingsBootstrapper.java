package it.distributedsystems.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.raft.BrokerSettings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.lang.System.exit;

/**
 * Class used to protect setters, called by the main app once at the start. Bootstrap of every final setting
 */
public class BrokerSettingsBootstrapper extends BrokerSettings {
    public static void bootstrap(int indexInFile){
        final Path path = Path.of("src/main/java/it/distributedsystems/brokerPortConfiguration.json");

        String jsonText = null;
        try {
            jsonText = Files.readString(path);
        } catch (IOException e) {
            System.out.println("Cannot find configuration file");
            exit(-1);
        }
        Gson gson = new Gson();

        List<BrokerAddress> newBrokerAddress = gson.fromJson(jsonText, new TypeToken<List<BrokerAddress>>() {}.getType());

        setBrokerAddress(newBrokerAddress.get(indexInFile+1));
        setBrokers(newBrokerAddress);
    }
}
