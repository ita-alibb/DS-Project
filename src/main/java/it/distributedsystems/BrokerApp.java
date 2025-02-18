package it.distributedsystems;

import it.distributedsystems.connection.BrokerConnection;
import it.distributedsystems.raft.BrokerSettings;
import it.distributedsystems.utils.BrokerSettingsBootstrapper;

public class BrokerApp {
    public static void main(String[] args) {
        //TODO: per lo startup si potrebbe fare che il primo leader viene hardcoded:
        //  passi un args se leader o no (ovviamente lo passi solo ad un nodo)
        //  fai partire prima tutti i Follower, questi rimarranno inattivi (fermi qui a BrokerApp)
        //  fino a quando il Leader non li contatta
        //  il Leader ha noto (con config file o con args IP e Port di tutti gli altri nodi che sono noti per design)
        BrokerSettingsBootstrapper.bootstrap(Integer.parseInt(args[0]));

        //Start-up connection and all threads relative to socket and message handling
        BrokerConnection.start();

        //Start TUI update thread

    }
}
