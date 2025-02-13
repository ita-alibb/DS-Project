package it.distributedsystems.raft;

import it.distributedsystems.commands.*;
import it.distributedsystems.utils.IndexedQueue;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class represent the model of the broker. It has the dictionary of the queues and the possible operation.
 * It is a singleton to be able to be called by different components easily and be sure that only one model per broker will be present
 */
public class BrokerModel {
    private static BrokerModel INSTANCE;

    /**
     * This lock must be called before every
     */
    private final ReentrantLock processCommandLock = new ReentrantLock();
    private String brokerID;

    /**
     * String: queueKey
     * IndexedQueue: the list of elements
     */
    private Dictionary<String, IndexedQueue> queues = new Hashtable<String, IndexedQueue>();

    /**
     * Singleton thread safe initialization
     */
    public synchronized static BrokerModel getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new BrokerModel();
        }

        return INSTANCE;
    }

    /**
     * Create a queue with the provided queueKey
     */
    private void createQueue(String queueKey) {
        this.queues.put(queueKey, new IndexedQueue());
    }

    /**
     * Append the data to the provided queueKey
     * @throws IllegalArgumentException if the queue is not found
     */
    private void appendData(String queueKey, int data) throws IllegalArgumentException {
        var queue = this.queues.get(queueKey);
        if (queue == null) {
            throw new IllegalArgumentException("Queue not found");
        }

        queue.appendData(data);
    }

    /**
     * Read the data from the queue and return it
     * @throws IllegalArgumentException if the queue is not found
     * @throws IndexOutOfBoundsException if the client has already read all the queue
     */
    private int readData(String queueKey, String clientID) throws IllegalArgumentException, IndexOutOfBoundsException {
        var queue = this.queues.get(queueKey);
        if (queue == null) {
            throw new IllegalArgumentException("Queue not found");
        }

        return queue.readData(clientID);
    }

    /**
     * Calls the method processCommandInternal, ensuring that who calls it has acquired the lock
     */
    public BaseResponse processCommand(ClientCommand command) {
        BaseResponse response;
        // Check if current thread already holds the lock. Here enters if you do not lock it manually.
        if (!processCommandLock.isHeldByCurrentThread()) {
            processCommandLock.lock();
            try {
                response = processCommandInternal(command);
            } finally {
                processCommandLock.unlock();
            }
        } else {
            // Already locked by this thread; call directly. Then you will need to unlock, used to call in batch.
            response = processCommandInternal(command);
        }

        return response;
    }

    private BaseResponse processCommandInternal(ClientCommand command) {
        try {
            switch (command.getType()) {
                case CREATE_QUEUE : createQueue(command.getQueueKey()); break;
                case APPEND_DATA : appendData(command.getQueueKey(), command.getData()); break;
                case READ_DATA : readData(command.getQueueKey(), command.getSenderID()); break;
            }
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            return new BaseResponse(this.brokerID,e.getMessage(), true);
        } catch (Exception e) {
            return new BaseResponse(this.brokerID,e.getClass().toString(), true);
        }

        return new BaseResponse(this.brokerID);
    }

    public void acquireLock(){
        processCommandLock.lock();
    }

    public void releaseLock(){
        processCommandLock.unlock();
    }
}
