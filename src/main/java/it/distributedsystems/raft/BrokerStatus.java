package it.distributedsystems.raft;

public enum BrokerStatus {
    /**
     * In this state you redirect client to the leader, and process the AppendEntries sent by the leader
     */
    Follower,

    /**
     * Temporary state, the Broker is trying to become the leader
     */
    Candidate,

    /**
     * Leader state, the only one that communicates with the clients and send handles Raft protocol
     */
    Leader
}
