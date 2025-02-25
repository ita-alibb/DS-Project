package it.distributedsystems.utils;

public class BrokerAddress {
    public int id;
    public String IP;
    public int ClientServerPort;
    public int BrokerServerPort;

    public BrokerAddress() {
    }

    public BrokerAddress(String IP, int clientServerPort) {
        this.IP = IP;
        ClientServerPort = clientServerPort;
    }

    public String addressStringForClient(){
        return String.format("%s:%d", IP, ClientServerPort);
    }
}
