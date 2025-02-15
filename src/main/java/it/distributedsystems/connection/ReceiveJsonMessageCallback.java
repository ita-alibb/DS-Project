package it.distributedsystems.connection;

@FunctionalInterface
public interface ReceiveJsonMessageCallback {
    void onReceiveJsonMessage(String jsonMessage) throws InterruptedException;
}