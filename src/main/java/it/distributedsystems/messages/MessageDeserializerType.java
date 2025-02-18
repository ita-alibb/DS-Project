package it.distributedsystems.messages;

public enum MessageDeserializerType {
    //Client<->Broker messages
    CONNECTION_MESSAGE,
    QUEUE_COMMAND,
    QUEUE_RESPONSE,
    CONNECTION_RESPONSE,
    //Broker<->Broker messages
    APPEND_ENTRIES,
}
