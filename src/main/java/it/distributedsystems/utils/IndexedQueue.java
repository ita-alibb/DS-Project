package it.distributedsystems.utils;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;

public class IndexedQueue {
    /**
     * The actual element of the queue
     */
    private final List<Integer> elements;
    private int maxElem = 0;

    /**
     * Dictionary to keep track the read pointer of every client
     * < clientID,nextElementToRead >
     */
    private final Dictionary<String, Integer> readPointer;

    public IndexedQueue() {
        this.elements = new ArrayList<Integer>();
        this.readPointer = new Hashtable<String, Integer>();
    }

    /**
     * Append data to the elements
     * @param data The data to be appended
     */
    public void appendData(int data) {
        this.elements.add(data);
        maxElem++;
    }

    /**
     * Read the correct data from the elements list. If the client id is not present in the readPointer it is added.
     * @param clientID The client that want to read this queue
     * @return the read value
     * @throws IndexOutOfBoundsException if the clientId has read all the queue
     */
    public int readData(String clientID) throws IndexOutOfBoundsException {
        var clientReadPointer = readPointer.get(clientID);
        if (clientReadPointer == null) {
            readPointer.put(clientID, 0);
            clientReadPointer = 0;
        }

        if (clientReadPointer >= maxElem) {
            throw new IndexOutOfBoundsException("Already Reached End of Queue");
        }

        var returnData = this.elements.get(clientReadPointer++);
        this.readPointer.put(clientID, clientReadPointer);

        return returnData;
    }
}
