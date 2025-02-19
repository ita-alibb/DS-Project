package it.distributedsystems.connection.handler;

import it.distributedsystems.connection.ReceiveJsonMessageCallback;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.Socket;

public class LeaderHandler extends SocketHandler {
    private final int leaderId;

    public LeaderHandler(int leaderId, Socket socket, PrintWriter out, BufferedReader in, ReceiveJsonMessageCallback msgReceiveCallback) {
        super(socket, out, in, msgReceiveCallback);
        this.leaderId = leaderId;
    }

    public int getLeaderId() {
        return leaderId;
    }
}
