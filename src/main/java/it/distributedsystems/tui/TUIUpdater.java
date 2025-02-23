package it.distributedsystems.tui;

import it.distributedsystems.connection.ClientConnection;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.raft.BrokerModel;

import java.util.stream.Collectors;

import static java.lang.System.exit;

public class TUIUpdater implements Runnable {
    private final boolean isClient;

    public TUIUpdater(boolean isClient) {
        this.isClient = isClient;
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        try {
            while (true) {
                Thread.sleep(1_000); //1 sec delay

                if (this.isClient) {
                    this.printClientViewInternal();
                } else {
                    this.printBrokerViewInternal();
                }
            }
        } catch (InterruptedException e) {
            exit(-1);
        }
    }

    /**
     * Actually reprints the view of the Client.
     */
    private void printClientViewInternal(){
        clearConsole();
        var ba = ClientConnection.getINSTANCE().getBrokerAddress();
        System.out.println("──────────────────────────────────────────────────────────────────────");
        System.out.printf("UserID: %d            Leader IP: %s Leader Port: %d                     %n", ClientConnection.getINSTANCE().getClientId(), ba.IP, ba.ClientServerPort);
        System.out.println("Last Read Int: "+ ClientConnection.getINSTANCE().getLastReadInt());
        System.out.println("List of not ack commands: " + ClientConnection.getINSTANCE().getSentCommands().stream().map(BaseDeserializableMessage::toJson).collect(Collectors.joining(", ")));
        System.out.println("Last Error: "+ ClientConnection.getINSTANCE().getLastError());
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
        //System.out.println("Last Error: "+ errorMessage);
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
        System.out.println("[ COMMANDS:");
        System.out.println("┌──────────────────────────────────────────────────────────────────────┐");
        System.out.println("│ - C {queueKey}         -> create specific queue                      │");
        System.out.println("│ - A {queueKey} {data}  -> append data to specified queue             │");
        System.out.println("│ - R {queueKey}         -> read data from the specified queue         │");
        System.out.println("└──────────────────────────────────────────────────────────────────────┘");
    }
}
