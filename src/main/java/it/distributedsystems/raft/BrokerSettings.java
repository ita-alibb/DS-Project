package it.distributedsystems.raft;

import it.distributedsystems.utils.BrokerAddress;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class BrokerSettings {
    //Final public static settings, settings that are set once and never changes
    public final static int APPEND_ENTRIES_TIME = 1_000; //1 second, period of sending of an Append_entries batch

    // Broker socket settings:
    private static BrokerAddress brokerAddress = null;
    private static int numOfNodes;

    //Broker RAFT settings:
    private static BrokerStatus brokerStatus = BrokerStatus.Follower;
    private static BrokerAddress leaderAddress = null;
    private static List<BrokerAddress> knownBrokers;

    public synchronized static BrokerStatus getBrokerStatus() {
        return brokerStatus;
    }

    public synchronized static void setBrokerStatus(BrokerStatus newBrokerStatus) {
        brokerStatus = newBrokerStatus;
    }

    public synchronized static BrokerAddress getLeaderAddress(){
        return leaderAddress;
    }

    public synchronized static void setLeaderAddress(int newLeaderId){
        leaderAddress = knownBrokers.stream().filter(ba -> ba.id == newLeaderId).findFirst().get();
    }

    /**
     * The list is composed of addresses of every other broker in this format "{IP}:{Port}"
     */
    public synchronized static List<BrokerAddress> getBrokers(boolean excludeMyself) {
        if (excludeMyself) {
            //remove myself from the set of known brokers
            return new LinkedList<>(knownBrokers.stream().filter(b -> b.id != brokerAddress.id).toList());
        } else {
            return new LinkedList<>(knownBrokers);
        }
    }

    public static int getBrokerID(){
        return brokerAddress.id;
    }

    public static BrokerAddress getBrokerAddress() {
        return brokerAddress;
    }

    public synchronized static String getBrokerIP(){
        if (brokerAddress.IP == null) {
            try {
                brokerAddress.IP = Inet4Address.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                throw new RuntimeException("Cannot get host address", e);
            }
        }

        return brokerAddress.IP;
    }

    public synchronized static int getNumOfNodes() {
        return numOfNodes;
    }

    protected synchronized static void setNumOfNodes(int numOfNodes) {
        BrokerSettings.numOfNodes = numOfNodes;
    }

    /**
     * Gets the port for Broker to Broker communication
     */
    public synchronized static int getBtoBPort(){
        return brokerAddress.BrokerServerPort;
    }

    /**
     * Gets the port for Client<=>Broker communication
     */
    public synchronized static int getCtoBPort(){
        return brokerAddress.ClientServerPort;
    }

    // region Protected Setters
    /**
     * Set the broker address, from global configuration on startup based on provided id in args
     */
    protected static void setBrokerAddress(BrokerAddress newBrokerAddress){
        brokerAddress = newBrokerAddress;
    }

    protected synchronized static void setBrokers(List<BrokerAddress> newKnownBrokers) {

        knownBrokers = newKnownBrokers;

    }
    // end-region
}
