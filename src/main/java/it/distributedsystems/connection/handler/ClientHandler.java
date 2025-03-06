package it.distributedsystems.connection.handler;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.sun.jdi.ClassNotPreparedException;
import it.distributedsystems.connection.BrokerConnection;
import it.distributedsystems.connection.ReceiveJsonMessageCallback;
import it.distributedsystems.messages.GsonDeserializer;
import it.distributedsystems.messages.queue.ConnectionMessage;
import it.distributedsystems.messages.queue.ConnectionResponse;
import it.distributedsystems.messages.queue.QueueResponse;
import it.distributedsystems.messages.raft.PastClientInfos;
import it.distributedsystems.raft.BrokerSettings;
import it.distributedsystems.raft.BrokerStatus;
import it.distributedsystems.utils.BrokerAddress;

import java.io.IOException;
import java.net.Socket;

public class ClientHandler extends SocketHandler {
    private final int clientId;

    private int lastCommandIdReceived = -1;

    private QueueResponse lastResponse = null;

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
        if (msg.getClientID() == -1){//new client
            this.clientId = BrokerConnection.getInstance().getNewClientId();
        } else {//reconnection
            var pastInfos = BrokerConnection.getInstance().getPastClientInfos(msg.getClientID());
            this.clientId = msg.getClientID();
            this.lastCommandIdReceived = pastInfos.getLastCommandId();
            this.lastResponse = pastInfos.getLastResponse();
        }

        //Send the response of established connection to the client
        var setOfBrokers = BrokerSettings.getBrokers(false); //put also myself
        this.sendMessage(new ConnectionResponse(this.clientId, setOfBrokers.stream().map(BrokerAddress::addressStringForClient).toList()));
    }

    @Override
    public void setMsgReceiveCallback(ReceiveJsonMessageCallback msgReceiveCallback) {
        if (this.msgReceiveCallback != null) {
            System.out.println("Cannot re set CallBack");
            return;
        }

        this.msgReceiveCallback = (jsonMessage) -> {
            var receivedCommandId = extractCommandID(jsonMessage);

            //should never happen
            if (receivedCommandId == null) throw new RuntimeException("CommandId here should never be null");

            //If reached here it has a queue command
            if (receivedCommandId <= lastCommandIdReceived ) {//cannot be less or equal, in both case just send the last response if present
                System.out.println("CommandId should increase monotonically justReceived = " + receivedCommandId + " lastCommandId = " + lastCommandIdReceived);
                if (lastResponse != null) {
                    this.sendMessage(lastResponse);
                }
                return;//special case: if last response is still null it means the command is being processed, so it should not be added in the queue
            } else if (receivedCommandId > lastCommandIdReceived){//new command, reset last response and set to the higher
                lastCommandIdReceived = receivedCommandId;//set to the highest
                lastResponse = null;
                //the message is a new message, append it to the queue
                msgReceiveCallback.onReceiveJsonMessage(jsonMessage);
            }
        };
    }

    /**
     * According to raft paper each client handler should keep track of the last id and last response,
     * to avoid that in case of leader change the same command is applied two times
     */
    private Integer extractCommandID(String jsonMessage) {
        try {
            JsonObject jsonObject = JsonParser.parseString(jsonMessage).getAsJsonObject();
            if (jsonObject.has("commandID")){
                return jsonObject.get("commandID").getAsInt();
            } else {
                System.out.println("Command is not a QueueCommand ---> Strange " + jsonMessage);
                return null;
            }
        } catch (JsonSyntaxException | IllegalStateException e) {
            System.out.println("Exception on get property commandID: " + e);
            return null; // Not a queue command
        }
    }

    public int getClientId() {
        return clientId;
    }

    @Override
    protected void customDisconnection() {
        BrokerConnection.getInstance().disconnectClientHandler(new PastClientInfos(this.clientId,this.lastCommandIdReceived,this.lastResponse));
    }

    public void setLastResponse(QueueResponse response) {
        this.lastResponse = response;
    }
}
