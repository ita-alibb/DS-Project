package it.distributedsystems.connection.handler;

import com.sun.jdi.ClassNotPreparedException;
import it.distributedsystems.connection.ReceiveJsonMessageCallback;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.queue.ConnectionMessage;
import it.distributedsystems.messages.raft.LeaderIdentification;
import it.distributedsystems.messages.raft.RequestVote;

import java.io.IOException;
import java.net.Socket;

public class LeaderHandler extends SocketHandler {
    private final int leaderId;

    public LeaderHandler(Socket socket) throws IOException, ClassNotPreparedException {
        super(socket);

        //Initialize connection
        BaseDeserializableMessage msg = GsonDeserializer.deserialize(in.readLine()); //receive the connection message

        if (msg instanceof RequestVote) {//Message from another follower (candidate) to RequestVote RPC
            // if conditions to vote, vote Yes else vote no
            handleRequestVote();

            //Close the socket. Vote or not, but then close the handler. If it becomes the leader you will be re-contacted
            throw new ClassNotPreparedException("Not Leader Connection");
        } else if (msg instanceof LeaderIdentification) {//Message from the leader, it connects to this follower.
            //create LeaderHandler to keep persistent connection
            this.leaderId = ((LeaderIdentification) msg).getLeaderId();
        } else {
            throw new ClassNotPreparedException("Not Leader Connection, message of class: " + msg.getClass());
        }
    }

    /**
     * If the message is a RequestVote than evaluate it and return. Not create instance of LeaderHandler
     */
    private void handleRequestVote() {

    }

    public int getLeaderId() {
        return leaderId;
    }

    public void shutDownLeaderHandler(){
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close(); // Close the socket
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
