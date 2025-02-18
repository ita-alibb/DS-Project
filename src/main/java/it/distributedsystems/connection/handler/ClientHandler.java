package it.distributedsystems.connection.handler;

import it.distributedsystems.connection.ReceiveJsonMessageCallback;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler extends SocketHandler {
    private final int clientId;


    public ClientHandler(int clientId, Socket socket, PrintWriter out, BufferedReader in, ReceiveJsonMessageCallback msgReceiveCallback) {
        super(socket, out, in, msgReceiveCallback);
        this.clientId = clientId;
    }

    public int getClientId() {
        return clientId;
    }
}
