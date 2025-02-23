package it.distributedsystems;

import it.distributedsystems.connection.ClientConnection;

import it.distributedsystems.tui.InputReader;
import it.distributedsystems.tui.TUIUpdater;

public class ClientApp {
    public static void main(String[] args) {
        //get client id if passed as parameters for restore after crash
        var clientID = (args.length > 2 && args[3] != null) ? Integer.parseInt(args[3]) : -1;
        //initialize connection
        ClientConnection.setConnection(args[0], args[1], clientID);

        //start listening thread
        new Thread(ClientConnection.getINSTANCE()).start();

        //start input reader
        InputReader.readLine();

        //Print tui
        var updater = new TUIUpdater(true);
        new Thread(updater).start();
    }
}
