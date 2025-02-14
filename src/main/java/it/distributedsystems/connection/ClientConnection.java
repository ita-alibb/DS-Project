package it.distributedsystems.connection;

import it.distributedsystems.messages.*;
import it.distributedsystems.messages.client.DataResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * This class handles the connection with the Broker net (the Leader)
 */
public class ClientConnection implements Runnable{
    /**
     * The clientID, it's -1 at the beginning, then it is updated with the one sent from the leader (only one change per lifetime)
     */
    private static int clientID = -1;
    /**
     * The client socket
     */
    private Socket socket;

    /**
     * The socket out stream
     */
    private PrintWriter out;

    /**
     * The socket in stream
     */
    private BufferedReader in;

    private final ExecutorService processResponseThread = Executors.newSingleThreadExecutor();

    /**
     * This list keeps tracks of sent commandIds, removed when ack is received
     */
    private final List<Integer> sentCommandIds = Collections.synchronizedList(new ArrayList<>());


    private final LinkedBlockingQueue<BaseResponse> asynchronousResponseQueue = new LinkedBlockingQueue<>();

    public ClientConnection(String serverIp, String tcpPort) {
        try {
            initConnection(serverIp,Integer.parseInt(tcpPort));
        } catch (IOException e) {
            System.out.println("Error initializing connection");
        }
    }

    private void initConnection(String serverIp, int tcpPort) throws IOException {
        String[] connectionResponse;
        do {
            System.out.printf("Client started with host %s and port %d %n", serverIp,tcpPort);
            // establish connection to server
            this.socket = new Socket(serverIp,tcpPort);

            this.out = new PrintWriter(this.socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));

            //Connection established
            //Send my id (if -1 it means I need the first id from the leader
            out.println(clientID);
            out.flush();//ensure sending

            //Wait for response 2 possible:
            // - "id:{clientID}" everything went well and I get the id (if I already have an id I will get the same),
            // - "leaderIP:{ip}:{port}" I was trying to connect to a follower, and so I get the leader IP and port
            // TODO: QUI SI DEVE FARE UNA CLASSE PER QUESTE RISPOSTE, PERCHE' DEVONO CONTENERA ANCHE LA LISTA DI IP:PORT DI OGNI ALTRO FOLLOWER NOTO AL LEADER. SERVE IN CASO DI CADUTA DEL LEADER PER SAPERE CHI IL CLIENT DEVE CONTATTARE
            connectionResponse = in.readLine().split(":");
            if (Objects.equals(connectionResponse[0].toLowerCase(), "leaderip")){
                serverIp = connectionResponse[1];
                tcpPort = Integer.parseInt(connectionResponse[2]);
            } else {
                clientID = Integer.parseInt(connectionResponse[1]);
            }
        } while (!Objects.equals(connectionResponse[0].toLowerCase(), "leaderip"));
    }

    public static int getClientId() {
        return clientID;
    }

    /**
     * Runs the listening thread of the socket.
     */
    @Override
    public void run() {
        // Initialize Listening thread
        System.out.println("Listening Thread started");

        // Start thread to process responses (not stop the listening on the socket)
        this.processResponseThread.execute(this::processResponseAsync);

        String jsonResponse;

        try {
            while((jsonResponse = in.readLine()) != null){
                /*System.out.println("received: " + jsonResponse);*/

                try {
                    BaseResponse res = GsonDeserializer.deserializeResponse(jsonResponse);

                    //put the response in the queue, will be processed by the processResponseThread
                    this.asynchronousResponseQueue.put(res);
                } catch (Exception e) {
                    System.out.println("Deserialize throw exception:" + e.getMessage());
                }
            }
        } catch (IOException e) {
            // break the loop and finally call the disconnection
        } finally {
            try {
                this.socket.close();
            } catch (IOException e) {
                System.out.println("Exception closing socket: exception: " + e.getMessage());
            }
        }
    }

    private void processResponseAsync() {
        while (true) {
            try {
                var response = this.asynchronousResponseQueue.take();

                if (response instanceof DataResponse) {
                    //TODO: get the data and process it (show in the tui something like LastReadData: x, and you here update that value that is the one shown in the tui)
                }

                //remove the command id from the uncommited command list:
                sentCommandIds.remove(Integer.valueOf(response.getCommandId()));
            } catch (InterruptedException e) {
                System.out.println("Error on executing broadcast");
            }
        }
    }
    /**
     * Synchronized method to be sure that the response is correct
     * Method used to send the command to the server
     *
     * @param command the command to send
     */
    private synchronized void sendAsync(BaseCommand command) {
        try {
            this.out.println(command.toJson());
            this.sentCommandIds.add(command.commandId);
        } catch (Exception e) {
            System.out.println("Exception on sending request to server; exception: " + e.getMessage());
        }
    }
}

