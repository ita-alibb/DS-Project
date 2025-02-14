package it.distributedsystems.raft;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class BrokerSettings {
    // Broker socket settings:
    private static String brokerIP = null;
    private static int brokerToBrokerPort = 0;

    //Broker RAFT settings:
    //TODO: initial status is FOLLOWER and then election, changed to leader only to test
    private static BrokerStatus brokerStatus = BrokerStatus.Leader;

    private static String leaderIP = "127.0.0.1";
    private static Integer leaderPort = 8080;
    private static List<String> knownBrokers;


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

    public static Object[] getLeaderAddress(){
        settingsLock.lock();
        Object[] returnVal = {leaderIP,leaderPort};
        settingsLock.unlock();
        return returnVal;
    }

    public static void setLeaderAddress(Object[] newLeaderAddress){
        settingsLock.lock();
        leaderIP = (String) newLeaderAddress[0];
        leaderPort = (Integer) newLeaderAddress[1];
        settingsLock.unlock();
    }

    /**
     * The list is composed of addresses of every other broker in this format "{IP}:{Port}"
     */
    public static List<String> getBrokers() {
        settingsLock.lock();
        var returnVal = new LinkedList<String>(knownBrokers);
        settingsLock.unlock();
        return returnVal;
    }

    public static void setBrokers(List<String> newKnownBrokers) {
        settingsLock.lock();
        knownBrokers.clear();
        knownBrokers.addAll(newKnownBrokers);
        knownBrokers.remove(brokerIP+":"+brokerToBrokerPort);
        knownBrokers.add(brokerIP+":"+brokerToBrokerPort);
        knownBrokers = knownBrokers.reversed(); //my ip is at position 0. This way is always good to iterate from 1 to N-1
        settingsLock.unlock();
    }

    public static String getBrokerIP(){
        if (brokerIP == null) {
            try {
                brokerIP = Inet4Address.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                throw new RuntimeException("Cannot get host address", e);
            }
        }

        return brokerIP;
    }

    /**
     * Gets the port for Broker to Broker communication
     */
    public static int getBtoBPort(){
        return brokerToBrokerPort;
    }

    /**
     * Gets the port for Client<=>Broker communication
     */
    public static int getCtoBPort(){
        return brokerToBrokerPort + 1;
    }

    /**
     * Set the broker tcp port for broker to broker (client to broker will be +1) from args on startup
     */
    public static void setBrokerToBrokerPort(int newPort){
        brokerToBrokerPort = newPort;
    }
}
