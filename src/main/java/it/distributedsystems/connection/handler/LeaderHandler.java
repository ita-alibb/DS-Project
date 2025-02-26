package it.distributedsystems.connection.handler;

import com.sun.jdi.ClassNotPreparedException;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.raft.LeaderIdentification;
import it.distributedsystems.messages.raft.RequestVote;
import it.distributedsystems.messages.raft.RequestVoteResponse;
import it.distributedsystems.raft.BrokerState;
import it.distributedsystems.raft.ReplicationLog;

import java.io.IOException;
import java.net.Socket;

public class LeaderHandler extends SocketHandler {
    private final int leaderId;

    public LeaderHandler(Socket socket) throws IOException, ClassNotPreparedException {
        super(socket);

        //Initialize connection
        BaseDeserializableMessage msg = GsonDeserializer.deserialize(in.readLine()); //receive the connection message

        if (msg instanceof RequestVote vote) {//Message from another follower (candidate) to RequestVote RPC
            // if conditions to vote, vote Yes else vote no
            handleRequestVote(vote);

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
    private void handleRequestVote(RequestVote msg) {
        var currentTerm = BrokerState.getCurrentTerm();
        boolean voteGranted = msg.getCandidateTerm() >= currentTerm;//if candidate term is less, it is outdated, not grant vote
        voteGranted = voteGranted && (BrokerState.getVotedFor() == null || BrokerState.getVotedFor() == msg.getCandidateTerm());
        voteGranted = voteGranted &&
                (msg.getCandidateLastLogTerm() >= ReplicationLog.getPrevLogLineTerm() &&
                        msg.getCandidateLastLogIndex() >= ReplicationLog.getPrevLogLineIndex());

        if (voteGranted) {
            BrokerState.setCurrentTerm(msg.getCandidateTerm());
            BrokerState.setVotedFor(msg.getCandidateId());
        }

        out.println(new RequestVoteResponse(currentTerm, voteGranted).toJson());
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
