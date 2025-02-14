package it.distributedsystems.raft;

public enum BrokerStatus {
    /**
     * In this state you redirect client to the leader, and process the AppendEntries sent by the leader
     */
    Follower,

    /**
     *
     */
    Candidate,
    Leader
}
