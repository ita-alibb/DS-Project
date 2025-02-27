package it.distributedsystems.connection.handler;

import com.sun.jdi.ClassNotPreparedException;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.raft.LeaderIdentification;
import it.distributedsystems.messages.raft.RequestVote;
import it.distributedsystems.messages.raft.RequestVoteResponse;
import it.distributedsystems.raft.BrokerSettings;
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
            var voteGranted = evaluateVoteGranted(vote);
            //send vote
            out.println(new RequestVoteResponse(BrokerSettings.getBrokerID(), BrokerState.getCurrentTerm(), voteGranted).toJson());

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
    private boolean evaluateVoteGranted(RequestVote msg) {
        //if candidate term is less, it is outdated, not grant vote
        if (msg.getCandidateTerm() < BrokerState.getCurrentTerm()) return false;

        //If already voted for someone else in this term reply false
        if (BrokerState.getVotedFor() != null && BrokerState.getVotedFor() != msg.getCandidateId()) return false;

        //if log is not up-to-date reply false
        if (msg.getCandidateLastLogTerm() < ReplicationLog.getLastLogLineTerm()) return false;//compare term

        if (msg.getCandidateLastLogIndex() < ReplicationLog.getLastLogLineIndex()) return false;//compare index

        //if reach here the vote is granted
        BrokerState.setCurrentTerm(msg.getCandidateTerm());
        BrokerState.setVotedFor(msg.getCandidateId());
        return true;
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
        } finally {
            System.out.println("Stopped previous leaderHandler");
        }
    }
}
