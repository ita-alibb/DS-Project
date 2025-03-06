package it.distributedsystems.tui;

import it.distributedsystems.connection.BrokerConnection;
import it.distributedsystems.connection.ClientConnection;
import it.distributedsystems.messages.BaseDeserializableMessage;
import it.distributedsystems.raft.*;

import java.util.stream.Collectors;

import static java.lang.System.exit;

public class TUIUpdater implements Runnable {
    //region Colors
    public static final String RESET = "\u001B[0m";
    public static final String RED = "\u001B[31m";
    public static final String GREEN = "\u001B[32m";
    public static final String YELLOW = "\u001B[33m";
    public static final String BLUE = "\u001B[34m";
    //endregion
    private final boolean isClient;
    private static boolean fullHistory = true;

    private static String lastMessage = "";
    public synchronized static void setLastMessage(String newLast) {
        lastMessage = newLast;
    }

    public TUIUpdater(boolean isClient, boolean fullHistoryParam) {
        this.isClient = isClient;
        fullHistory = fullHistoryParam;
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        if (this.isClient) {
            printClientViewInternal();
            return;//disable client fixed update
        }
        try {
            while (true) {
                Thread.sleep(1000); //0.1 sec delay

                printBrokerViewInternal();
            }
        } catch (Exception ignored) {
            exit(-1);
        } finally {
            System.out.println("TUIUpdater STOPPED");
        }
    }

    /**
     * Actually reprints the view of the Client.
     */
    public static void printClientViewInternal(){
        clearConsole();
        var ba = ClientConnection.getINSTANCE().getBrokerAddress();
        System.out.println("──────────────────────────────────────────────────────────────────────");
        System.out.printf("UserID: %d            Leader IP: %s Leader Port: %d                     %n", ClientConnection.getINSTANCE().getClientId(), ba.IP, ba.ClientServerPort);
        System.out.println("Last Read Int: "+ ClientConnection.getINSTANCE().getLastReadInts().stream().map(String::valueOf)
                .collect(Collectors.joining(",")));
        System.out.println("List of not ack commands: " + ClientConnection.getINSTANCE().getSentCommands().stream().map(BaseDeserializableMessage::toJson).collect(Collectors.joining(", ")));
        System.out.println("Last Error: "+ ClientConnection.getINSTANCE().getLastError());
        printCommands();
    }

    /**
     * Actually reprints the view of the Broker.
     */
    private static void printBrokerViewInternal(){
        clearConsole();
        System.out.println("Time: " + System.currentTimeMillis());
        System.out.println("──────────────────────────────────────────────────────────────────────");

        var la = BrokerSettings.getLeaderAddress();
        System.out.printf("LeaderID: %d | Leader IP: %s | Leader Port: %d %n",la != null ? la.id : -1, la != null ? la.IP : -1, la != null ? la.ClientServerPort : -1);

        var ba = BrokerSettings.getBrokerAddress();
        System.out.printf("BrokerID: %d | Broker IP: %s | Broker Port: %d %n",ba.id, ba.IP, ba.ClientServerPort);
        System.out.printf("Broker Status: "+RED+"%s"+RESET+"   BrokerEpoch: %d  BrokerTimeout: "+YELLOW+"%s"+RESET+" %n", BrokerSettings.getBrokerStatus(), BrokerState.getCurrentTerm(), BrokerConnection.getInstance().getWaitTimeForCurrentTimer());
        System.out.printf("Broker CommitIndex: %d   LastApplied: %d  VotedForInCurrentTerm: %d %n", BrokerState.getCommitIndex(),BrokerState.getLastApplied(), BrokerState.getVotedFor());
        System.out.println("──────────────────────────────────────────────────────────────────────");

        if (BrokerSettings.getBrokerStatus() == BrokerStatus.Candidate){
            System.out.println(GREEN+"Election in progress..."+RESET);
            System.out.printf("#ofBrokers: %d   BrokerVotes-> Accept: %s  Deny: %s %n", BrokerSettings.getNumOfNodes(),
                    BrokerConnection.getInstance().getAcceptedCount().stream().map(String::valueOf)
                            .collect(Collectors.joining(",")),
                    BrokerConnection.getInstance().getDeniedCount().stream().map(String::valueOf)
                            .collect(Collectors.joining(",")));
        } else {
            if (BrokerSettings.getBrokerStatus() == BrokerStatus.Leader) {
                var followers = BrokerConnection.getInstance().getFollowers();
                System.out.printf("Connected Followers: ---------------%n");
                for (var follower : followers) {
                    System.out.printf(YELLOW + "Follower Id: %d Follower MatchIndex: %d" + RESET + "%n", follower.getFollowerId(), follower.getMatchIndex());
                }
                System.out.printf("------------------------------------%n");

                System.out.printf("Last polled command: %s %n", BrokerConnection.getInstance().getLastQueueCommand());
                System.out.println("──────────────────────────────────────────────────────────────────────");
            }

            System.out.printf(BLUE + "Last log line "+ RESET + "Index: %d Term: %d %n", ReplicationLog.getLastLogLineIndex(), ReplicationLog.getLastLogLineTerm());
            var queues = BrokerModel.getInstance().getQueues();
            System.out.println();
            System.out.println("Queues:");
            System.out.println(BLUE+"Key:"+ RESET + GREEN + " Values..."+RESET);
            System.out.println("-------------------------------------");
            for (var queueKey : queues.keySet()) {
                System.out.printf(BLUE + "%s :"+ RESET + GREEN + " %s"+ RESET + "%n", queueKey, queues.get(queueKey).toString());
                System.out.println("-------------------------------------");
            }
            System.out.println();
        }

        System.out.printf(GREEN+"Last message to show:"+RESET+" %s %n", lastMessage);
        lastMessage = "";
    }

    /**
     * Method to clear the console before a new TUI is printed. Usually, this method is called before each update, obviously only if the update wants the terminal to be updated.
     */
    private static void clearConsole(){
        if (fullHistory) {
            System.out.println();
            System.out.println();
            System.out.println("####################FULL HISTORY SET TO TRUE, DO NOT CLEAR####################");
            System.out.println();
            System.out.println();
        } else {
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
    }

    private static void printCommands(){
        System.out.println("[ COMMANDS:");
        System.out.println("┌──────────────────────────────────────────────────────────────────────┐");
        System.out.println("│ - C {queueKey}         -> create specific queue                      │");
        System.out.println("│ - A {queueKey} {data}  -> append data to specified queue             │");
        System.out.println("│ - R {queueKey}         -> read data from the specified queue         │");
        System.out.println("└──────────────────────────────────────────────────────────────────────┘");
    }
}
