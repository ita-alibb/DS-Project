package it.distributedsystems.tui;

import it.distributedsystems.connection.BrokerConnection;
import it.distributedsystems.connection.ClientConnection;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.raft.BrokerModel;
import it.distributedsystems.raft.BrokerSettings;
import it.distributedsystems.raft.BrokerState;
import it.distributedsystems.raft.BrokerStatus;

import java.util.stream.Collectors;

import static java.lang.System.exit;

public class TUIUpdater implements Runnable {
    private final boolean isClient;

    private static String lastMessage = "";
    public static void setLastMessage(String newLast) {
        lastMessage = newLast;
    }

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
                Thread.sleep(1000); //0.1 sec delay

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
        var la = BrokerSettings.getLeaderAddress();
        var ba = BrokerSettings.getBrokerAddress();
        System.out.println("──────────────────────────────────────────────────────────────────────");
        System.out.printf("LeaderID: %d | Leader IP: %s | Leader Port: %d %n",la != null ? la.id : -1, la != null ? la.IP : -1, la != null ? la.ClientServerPort : -1);
        System.out.printf("BrokerID: %d | Broker IP: %s | Broker Port: %d %n",ba.id, ba.IP, ba.ClientServerPort);
        System.out.printf("Broker Status: %s   BrokerEpoch: %d  BrokerTimeout: %d %n", BrokerSettings.getBrokerStatus(), BrokerState.getCurrentTerm(), BrokerConnection.getInstance().getWaitTimeForCurrentTimer());
        System.out.printf("Broker CommitIndex: %d   LastApplied: %d  VotedForInCurrentTerm: %d %n", BrokerState.getCommitIndex(),BrokerState.getLastApplied(), BrokerState.getVotedFor());
        System.out.println("──────────────────────────────────────────────────────────────────────");

        if (BrokerSettings.getBrokerStatus() == BrokerStatus.Candidate){
            System.out.println("Election in progress...");
            System.out.printf("#ofBrokers: %d   Accept: %d  Deny: %d %n", BrokerSettings.getNumOfNodes(), BrokerConnection.getInstance().getAcceptedCount(),BrokerConnection.getInstance().getDeniedCount());
        } else {
            if (BrokerSettings.getBrokerStatus() == BrokerStatus.Leader) {
                System.out.printf("Last polled command: %s %n", BrokerConnection.getInstance().getLastQueueCommand());
                System.out.println("──────────────────────────────────────────────────────────────────────");
            }
            System.out.println("Queue Key: Values...");
            for (var queueKey : queues.keySet()) {
                System.out.printf(" %s : %s %n", queueKey, queues.get(queueKey).toString());
            }
        }

        System.out.printf("Last message to show: %s %n", lastMessage);
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
