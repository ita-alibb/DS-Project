package it.distributedsystems.raft;

import it.distributedsystems.utils.BrokerAddress;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class BrokerSettings {
    // Broker socket settings:
    private static BrokerAddress brokerAddress = null;
    private static int numOfNodes;

    //Broker RAFT settings:
    /**
     * Broker Epoch is -1 until first connected with the leader. Then represent the perceived epoch of the Broker
     */
    private static int brokerEpoch = -1;
    private static int brokerCommitIndex = -1;
    //TODO: initial status is FOLLOWER and then election, changed to leader only to test
    private static BrokerStatus brokerStatus = BrokerStatus.Follower;

    private static BrokerAddress leaderAddress = null;
    private static List<BrokerAddress> knownBrokers;


    private static final ReentrantLock settingsLock = new ReentrantLock();

    public static BrokerStatus getBrokerStatus() {
        settingsLock.lock();
        var returnVal = brokerStatus;
        settingsLock.unlock();

        return returnVal;
    }

    public static void setBrokerStatus(BrokerStatus newBrokerStatus) {
        settingsLock.lock();
        brokerStatus = newBrokerStatus;
        settingsLock.unlock();
    }

    public static BrokerAddress getLeaderAddress(){
        settingsLock.lock();
        var returnVal = leaderAddress;
        settingsLock.unlock();
        return returnVal;
    }

    public static void setLeaderAddress(int newLeaderId){
        settingsLock.lock();
        leaderAddress = knownBrokers.get(newLeaderId+1);
        settingsLock.unlock();
    }

    /**
     * The list is composed of addresses of every other broker in this format "{IP}:{Port}"
     */
    public static List<BrokerAddress> getBrokers() {
        settingsLock.lock();
        var returnVal = new LinkedList<>(knownBrokers);
        settingsLock.unlock();
        return returnVal;
    }

    public static int getBrokerID(){
        settingsLock.lock();
        var returnVal = brokerAddress.id;
        settingsLock.unlock();
        return returnVal;
    }

    public static String getBrokerIP(){
        if (brokerAddress.IP == null) {
            try {
                brokerAddress.IP = Inet4Address.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                throw new RuntimeException("Cannot get host address", e);
            }
        }

        return brokerAddress.IP;
    }

    public static int getNumOfNodes() {
        return numOfNodes;
    }

    protected static void setNumOfNodes(int numOfNodes) {
        BrokerSettings.numOfNodes = numOfNodes;
    }

    /**
     * Gets the port for Broker to Broker communication
     */
    public static int getBtoBPort(){
        return brokerAddress.BrokerServerPort;
    }

    /**
     * Gets the port for Client<=>Broker communication
     */
    public static int getCtoBPort(){
        return brokerAddress.ClientServerPort;
    }

    public static int getBrokerEpoch() {
        settingsLock.lock();
        var returnVal = brokerEpoch;
        settingsLock.unlock();
        return returnVal;
    }

    public static void setBrokerEpoch(int newBrokerEpoch) {
        settingsLock.lock();
        brokerEpoch = newBrokerEpoch;
        settingsLock.unlock();
    }

    public static int getBrokerCommitIndex() {
        settingsLock.lock();
        var returnVal = brokerCommitIndex;
        settingsLock.unlock();
        return returnVal;
    }

    public static void setBrokerCommitIndex(int brokerCommitIndex) {
        settingsLock.lock();
        BrokerSettings.brokerCommitIndex = brokerCommitIndex;
        settingsLock.unlock();
    }

    // region Protected Setters
    /**
     * Set the broker address, from global configuration on startup based on provided id in args
     */
    protected static void setBrokerAddress(BrokerAddress newBrokerAddress){
        brokerAddress = newBrokerAddress;
    }

    protected static void setBrokers(List<BrokerAddress> newKnownBrokers) {
        settingsLock.lock();
        knownBrokers = newKnownBrokers;
        settingsLock.unlock();
    }
    // end-region
}
