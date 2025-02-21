package it.distributedsystems.tui;

import it.distributedsystems.connection.ClientConnection;
import it.distributedsystems.messages.queue.CommandType;
import it.distributedsystems.messages.queue.QueueCommand;
import it.distributedsystems.raft.BrokerModel;
import it.distributedsystems.raft.LogLine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.nio.file.Path;
import java.util.Scanner;
import java.util.concurrent.*;

/**
 * This class is used to read the input stream.
 */
public class InputReader implements Runnable {
    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final Scanner scanner;

    private InputReader() {
        this.scanner = new Scanner(System.in);
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        try {
            //Choose Strategy
            String input;
            do {
                input = scanner.nextLine();
            } while (input == null);

            if (input.equals("test")) {
                runTest();
                return;
            }

            this.executeCommand(input);
        } catch (Exception e) {
            System.out.println("Error: incorrect command");
            System.out.print("> ");
        } finally {
            readLine();
        }
    }

    /**
     * The method used to handle only one thread in Stream.in.
     */
    public static void readLine() {
        executorService.execute(new InputReader());
    }

    /**
     * The method to set the Strategy and execute it.
     * @param input     The command given by the player
     */
    private void executeCommand(String input) {
        QueueCommand command = null;
        boolean error = false;

        var args = input.split("\\s+");

        switch (args[0]) {
            case "C" : {
                //create queue, I expect C {queueKey}
                if (args.length != 2) {
                    error = true;
                } else {
                    command = new QueueCommand(ClientConnection.getClientId(), CommandType.CREATE_QUEUE,args[1],null);
                }
            }; break;

            case "A": {
                //append data, I expect A {queueKey} {data}
                if (args.length != 3) {
                    error = true;
                } else {
                    //TODO: al momento parseInt non e' safe, io farei una versione safe che restituisce null in caso non sia parsable
                    command = new QueueCommand(ClientConnection.getClientId(),CommandType.APPEND_DATA,args[1],Integer.parseInt(args[2]));
                }
            }; break;

            case "R": {
                //read data, I expect R {queueKey}
                if (args.length != 2) {
                    error = true;
                } else {
                    command = new QueueCommand(ClientConnection.getClientId(),CommandType.READ_DATA,args[1],null);
                }
            }; break;
            default: error = true;
        }

        if (error) {
            System.out.println("Unknown command: " + input + " retry:");
            System.out.print("> ");
        } else {
            //send the command
            ClientConnection.getINSTANCE().sendAsync(command);
        }
    }

    private void runTest() {
        var file = "src/test/java/it/distributedsystems/testFile.txt";
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String command;
            while ((command = reader.readLine()) != null) {
                executeCommand(command);
            }
        } catch (InvalidObjectException e) {
            System.err.println("Error while extracting the command: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error while reading file: " + e.getMessage());
        }
    }
}
