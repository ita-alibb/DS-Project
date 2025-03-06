package it.distributedsystems;

import it.distributedsystems.connection.BrokerConnection;
import it.distributedsystems.raft.ReplicationLog;
import it.distributedsystems.tui.TUIUpdater;
import it.distributedsystems.utils.BrokerSettingsBootstrapper;

public class BrokerApp {
    public static void main(String[] args) {
        BrokerSettingsBootstrapper.bootstrap(Integer.parseInt(args[0]));

        //Start-up connection and all threads relative to socket and message handling
        BrokerConnection.start();

        //Start up the ReplicationLog. Something like the last commit index, the last epoch registered in log ecc...
        ReplicationLog.initializeLogFile();

        //Print TUI
        var fullHistory = args.length == 1 || args[1] == null || Boolean.parseBoolean(args[1]);

        var updater = new TUIUpdater(false, fullHistory);
        new Thread(updater).start();
    }
}
