package it.distributedsystems.messages;

public enum MessageDeserializerType {
    //Client<->Broker messages
    CONNECTION_MESSAGE,
    QUEUE_COMMAND,
    QUEUE_RESPONSE,
    CONNECTION_RESPONSE,
    //Broker<->Broker messages
    APPEND_ENTRIES,
    LEADER_IDENTIFICATION,
    APPEND_ENTRIES_RESPONSE,//both for ACK and NACK
    REQUEST_VOTE
}
