package it.distributedsystems;

import it.distributedsystems.connection.ClientConnection;

import it.distributedsystems.tui.InputReader;
import it.distributedsystems.tui.TUIUpdater;

public class ClientApp {
    public static void main(String[] args) {
        //initialize connection
        ClientConnection.setConnection(args[0], args[1]);

        //start listening thread
        new Thread(ClientConnection.getINSTANCE()).start();

        //start input reader
        InputReader.readLine();

        //Print tui
        var updater = new TUIUpdater(true);
        new Thread(updater).start();
    }
}
