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
        var updater = new TUIUpdater(false);
        new Thread(updater).start();
    }
}

//TODO: lastAppliedIndex and LastCommitIndex. Pensali meglio, tutti questi indici vanno tenuti in maniera piu' ordinata
// fosse anche solo raggruppandoli in classi che poi vanno dentro brokerSettings.
// lastApplied: tiene traccia dell'ultimo index applicato al model.
// commitIndex: tiene conto dell'ultimo index committato. (questo viene aggiornato e poi se e' piu' avanti del lastapplied allora si applicano tutti quelli in mezzo)
// lastIndex: tiene traccia dell'ultimo index inserito.
//
