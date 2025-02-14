package it.distributedsystems.messages.queue;

import java.util.List;

/**
 * This response is sent by a Broker on connection, if it is not the leader the bool is true and the dict empty
 */
public class ConnectionResponse extends ConnectionMessage {
    /**
     * Bool indicating whether it is a redirect response
     */
    private final boolean isRedirect;

    /**
     * The IP of the leader to reconnect
     */
    private final String leaderIP;

    /**
     * The port of the leader to reconnect
     */
    private final int leaderPort;

    /**
     * List of strings formatted like "ip:port" for every other known broker
     */
    private List<String> otherBrokers;

    /**
     * Constructor for Redirect response
     */
    public ConnectionResponse(String leaderIP, int leaderPort) {
        super(-1, MessageDeserializerType.CONNECTION_RESPONSE);
        this.isRedirect = true;
        this.leaderIP = leaderIP;
        this.leaderPort = leaderPort;
    }

    /**
     * Constructor for correct connection response
     */
    public ConnectionResponse(int clientID, List<String> otherBrokers) {
        super(clientID, MessageDeserializerType.CONNECTION_RESPONSE);
        this.isRedirect = false;
        this.otherBrokers = otherBrokers;
        this.leaderIP = null;
        this.leaderPort = 0;
    }

    public String getLeaderIP() {
        return leaderIP;
    }

    public int getLeaderPort() {
        return leaderPort;
    }
}
