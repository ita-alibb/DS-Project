package it.distributedsystems.connection.handler;

import it.distributedsystems.connection.ReceiveJsonMessageCallback;

import java.io.IOException;
import java.net.Socket;

public class FollowerHandler extends SocketHandler {
    /**
     * The broker id (Follower)
     */
    private final int followerId;

    /**
     * This is the next index to send.
     * It is used to keep track of how much AppendEntries you have to send.
     * Updated after receiving ACK
     */
    private int nextIndex;

    public FollowerHandler(int followerId, int leaderLastIndex, Socket socket, ReceiveJsonMessageCallback msgReceiveCallback) throws IOException {
        super(socket, msgReceiveCallback);
        this.followerId = followerId;
        this.nextIndex = leaderLastIndex +1;
    }

    public int getFollowerId() {
        return followerId;
    }

    public int getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(int nextIndex) {
        this.nextIndex = nextIndex;
    }
}
