package it.distributedsystems.messages.newmessages;

/**
 * This response is sent by a Broker on connection if it is not the leader
 */
public class RedirectResponse extends BaseDeserializableMessage {
    /**
     * The IP of the leader to reconnect
     */
    private final String leaderIP;

    /**
     * The port of the leader to reconnect
     */
    private final int leaderPort;

    public RedirectResponse(String leaderIP, int leaderPort) {
        super(MessageDeserializerType.REDIRECT_RESPONSE);
        this.leaderIP = leaderIP;
        this.leaderPort = leaderPort;
    }

    public String getLeaderIP() {
        return leaderIP;
    }

    public int getLeaderPort() {
        return leaderPort;
    }
}
