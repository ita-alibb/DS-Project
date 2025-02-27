package it.distributedsystems.connection.handler;

import com.sun.jdi.ClassNotPreparedException;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.queue.ConnectionMessage;
import it.distributedsystems.messages.queue.ConnectionResponse;
import it.distributedsystems.raft.BrokerSettings;
import it.distributedsystems.raft.BrokerStatus;
import it.distributedsystems.utils.BrokerAddress;

import java.io.IOException;
import java.net.Socket;

public class ClientHandler extends SocketHandler {
    /**
     * The next client ID that will be assigned to the next clients that will connect
     */
    private static int nextClientId = 1;

    private final int clientId;


    public ClientHandler(Socket socket) throws IOException, ClassNotPreparedException {
        super(socket);

        //Initialize connection
        ConnectionMessage msg = (ConnectionMessage) GsonDeserializer.deserialize(in.readLine()); //receive the connection message

        if (BrokerSettings.getBrokerStatus() != BrokerStatus.Leader) {
            //I am NOT the leader, so I can't connect with the client.
            //Send back the IP/Port of the leader
            var leaderAddress = BrokerSettings.getLeaderAddress();
            //Send error response
            this.sendMessage(new ConnectionResponse(leaderAddress));
            //Close the socket
            throw new ClassNotPreparedException("Not Leader");
        }

        //Handle the assign of an ID.
        //Send back the client ID. (nextClientId++)
        //TODO: in theory the reconnection of a client should be more robust:
        // current problems that i think are not worth the effort:
        // try connection with same given ID, there is no control on duplicated ID.
        this.clientId = msg.getClientID() == -1 ? nextClientId++ : msg.getClientID();

        //Send the response of established connection to the client
        var setOfBrokers = BrokerSettings.getBrokers(false); //put also myself
        this.sendMessage(new ConnectionResponse(this.clientId, setOfBrokers.stream().map(BrokerAddress::addressStringForClient).toList()));
    }

    public int getClientId() {
        return clientId;
    }

    /**
     * Used at restart
     */
    public static void setNextClientId(int updatedNextClientId){
        nextClientId = updatedNextClientId;
    }
}
