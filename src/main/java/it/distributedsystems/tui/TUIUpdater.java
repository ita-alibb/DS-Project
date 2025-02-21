package it.distributedsystems.tui;

import it.distributedsystems.connection.ClientConnection;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.raft.BrokerModel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class TUIUpdater {
    private static TUIUpdater INSTANCE;
    private String errorMessage;

    private ExecutorService reprinter = Executors.newSingleThreadExecutor();

    private TUIUpdater() {}

    public static TUIUpdater getINSTANCE() {
        if (INSTANCE == null) {
            INSTANCE = new TUIUpdater();
        }
        return INSTANCE;
    }

    /**
     * This method is called everytime something new must be shown in the view
     * Starts a single thread (cannot be started multiple consecutive times because if a thread is already running then will probably include the second change without starting another thread)
     */
    public void reprintViewAsync(boolean isClient) {
        //Se c'e' un thread gia' in esecuzione, RETURN senza startare
        //Starta un thread che fa printViewInternal
        if (isClient) {
            this.reprinter.execute(this::printClientViewInternal);
        } else {
            this.reprinter.execute(this::printBrokerViewInternal);
        }
    }

    public void printError(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * Actually reprints the view of the Client.
     */
    private void printClientViewInternal(){
        clearConsole();

        System.out.println("┌──────────────────────────────────────────────────────────────────────┐");
        System.out.printf("UserID: %d                                                              %n", ClientConnection.getClientId());
        System.out.println("List of not ack commands: " + ClientConnection.getINSTANCE().getSentCommands().stream().map(BaseDeserializableMessage::toJson).collect(Collectors.joining(", ")));
        System.out.println("Last Error: "+ errorMessage);
        printCommands();
    }

    /**
     * Actually reprints the view of the Broker.
     */
    private void printBrokerViewInternal(){
        clearConsole();

        var queues = BrokerModel.getInstance().getQueues();

        System.out.println("Queue Key: Values...");
        for (var queueKey : queues.keySet()) {
            System.out.printf(" %s : %s %n", queueKey, queues.get(queueKey).toString());
        }
        System.out.println("Last Error: "+ errorMessage);
    }

    /**
     * Method to clear the console before a new TUI is printed. Usually, this method is called before each update, obviously only if the update wants the terminal to be updated.
     */
    private void clearConsole(){
        try {
            final String os = System.getProperty("os.name");
            if (os.contains("Windows")) {
                new ProcessBuilder("cmd", "/c", "cls").inheritIO().start().waitFor();
            }
            else {
                Runtime.getRuntime().exec("clear");
            }
        } catch (Exception e) {
            System.out.println("\033[H\033[2J");
        }
    }

    private void printCommands(){
        System.out.println("┌──────────────────────────────────────────────────────────────────────┐");
        System.out.println("│                             COMMANDS                                 │");
        System.out.println("├──────────────────────────────────────────────────────────────────────┤");
        System.out.println("│ - C {queueKey}         -> create specific queue                      │");
        System.out.println("│ - A {queueKey} {data}  -> append data to specified queue             │");
        System.out.println("│ - R {queueKey}         -> read data from the specified queue         │");
        System.out.println("└──────────────────────────────────────────────────────────────────────┘");
    }
}
