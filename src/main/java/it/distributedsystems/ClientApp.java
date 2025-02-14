package it.distributedsystems;

import it.distributedsystems.connection.ClientConnection;

import it.distributedsystems.tui.InputReader;

public class ClientApp {
    public static void main(String[] args) {
        //initialize connection
        var connection = new ClientConnection(args[0], args[1]);

        //start listening thread
        new Thread(connection).start();

        //start input reader
        InputReader.readLine();
    }
}
