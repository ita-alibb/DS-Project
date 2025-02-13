package it.distributedsystems.connection;

import com.google.gson.Gson;
import it.distributedsystems.commands.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.*;

public class ConnectionTCP implements Runnable{

    private static ConnectionTCP INSTANCE;
    /**
     * The client socket
     */
    private final Socket socket;

    /**
     * The socket out stream
     */
    private final PrintWriter out;

    /**
     * The socket in stream
     */
    private final BufferedReader in;

    private final ExecutorService broadcastThread = Executors.newSingleThreadExecutor();

    private final LinkedBlockingQueue<BaseResponseData> asyncronousResponseQueue = new LinkedBlockingQueue<>();

    /**
     * The object needed to pass the response between two threads
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/SynchronousQueue.html">SynchronousQueue API</a>
     * Although the SynchronousQueue has an interface of a queue, we should think about it as an exchange point for a single element between two threads,
     * in which one thread is handing off an element, and another thread is taking that element.
     */
    private final SynchronousQueue<JsonMessage<BaseResponseData>> synchronousResponseQueue;

    public ConnectionTCP(String serverIp, int tcpPort) throws IOException {
        System.out.printf("Client started with host %s and port %d %n", serverIp,tcpPort);
        // establish connection to server
        this.socket = new Socket(serverIp,tcpPort);

        this.out = new PrintWriter(this.socket.getOutputStream(), true);
        this.in = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));

        this.synchronousResponseQueue = new SynchronousQueue<>();
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        System.out.println("Listening Thread started");

        // Start thread to send broadcast messages
        this.broadcastThread.execute(this::executeBroadcastAsync);
        // Initialize Listening thread
        String jsonResponse;

        try {
            while((jsonResponse = in.readLine()) != null){
                /*System.out.println("received: " + jsonResponse);*/

                try {
                    Gson gson = new Gson();
                    var res = gson.fromJson(jsonResponse);

                    if (res.getData().getIsBroadcast()) {
                        this.asyncronousResponseQueue.put(res.getData());
                    } else {
                        // If the response is not a broadcast then one of the ActionRMI method is waiting for the response to be added in the queue
                        // the offer methods inserts the element in the queue only if a thread is waiting for it, timeout handled
                        if (!synchronousResponseQueue.offer(res)){
                            System.out.println("Request thread timed out");
                        }
                    }
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

    /**
     * Synchronized method to be sure that the response is correct
     * Method used to send the command to the server
     *
     * @param command the command to send
     * @return the BaseResponseData
     */
    private synchronized BaseResponse send(BaseCommand command) {
        try {
            this.out.println(command.toJson());

            // Wait the response to be received, 1 minute timeout
            return this.synchronousResponseQueue.poll(1, TimeUnit.MINUTES).getData();
        } catch (NullPointerException e){
            System.out.println("Request Timeout");
        } catch (Exception e) {
            System.out.println("Exception on sending request to server; exception: " + e.getMessage());
        }
        return null;
    }

    private void executeBroadcastAsync() {
        while (true) {
            try {
                var broadcast = this.asyncronousResponseQueue.take();

                ViewModelState.getInstance().broadcastUpdate(broadcast);
            } catch (InterruptedException e) {
                System.out.println("Error on executing broadcast");
            }
        }
    }
}

