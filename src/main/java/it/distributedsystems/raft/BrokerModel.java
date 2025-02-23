package it.distributedsystems.raft;

import it.distributedsystems.messages.queue.QueueCommand;
import it.distributedsystems.messages.queue.QueueResponse;
import it.distributedsystems.tui.TUIUpdater;
import it.distributedsystems.utils.IndexedQueue;

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

    /**
     * String: queueKey
     * IndexedQueue: the list of elements
     */
    private final Hashtable<String, IndexedQueue> queues = new Hashtable<>();

    public BrokerModel() {
    }

    /**
     * Singleton thread safe initialization
     */
    public synchronized static BrokerModel getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new BrokerModel();
        }

        return INSTANCE;
    }

    public void acquireLock(){
        processCommandLock.lock();
    }

    public void releaseLock(){
        processCommandLock.unlock();
    }

    /**
     * Calls the method processCommandInternal, ensuring that who calls it has acquired the lock
     */
    public QueueResponse processCommand(QueueCommand command) {
        QueueResponse response;
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

        //Trigger update of the TUI


        return response;
    }

    private QueueResponse processCommandInternal(QueueCommand command) {
        Integer data = null;
        try {
            switch (command.getType()) {
                case CREATE_QUEUE : createQueue(command.getQueueKey()); break;
                case APPEND_DATA : appendData(command.getQueueKey(), command.getData()); break;
                case READ_DATA : data = readData(command.getQueueKey(), command.getClientID()); break;
            }
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            return new QueueResponse(command.getClientID(), command.getCommandID(), e.getMessage());
        } catch (Exception e) {
            return new QueueResponse(command.getClientID(), command.getCommandID(), e.getClass().toString());
        }

        return new QueueResponse(command.getClientID(), command.getCommandID(), data);
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
    private int readData(String queueKey, int clientID) throws IllegalArgumentException, IndexOutOfBoundsException {
        var queue = this.queues.get(queueKey);
        if (queue == null) {
            throw new IllegalArgumentException("Queue not found");
        }

        return queue.readData(clientID+"");
    }


    public Hashtable<String, IndexedQueue> getQueues() {
        processCommandLock.lock();
        var returnVal = new Hashtable<>(queues);
        processCommandLock.unlock();
        return returnVal;
    }
}
