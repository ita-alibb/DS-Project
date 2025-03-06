package it.distributedsystems.utils;

import it.distributedsystems.raft.BrokerSettings;
import it.distributedsystems.raft.BrokerStatus;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static it.distributedsystems.raft.BrokerSettings.APPEND_ENTRIES_TIME;

public class ElectionTimer {
    private final ScheduledThreadPoolExecutor timer;
    private final ScheduledThreadPoolExecutor delayedCancellation;
    private final Runnable task;

    // This will be used to track the current task execution Thread
    private final AtomicReference<Thread> currentTaskThread = new AtomicReference<>(null);

    public ElectionTimer(Runnable task) {
        // One execution at a time
        this.timer = new ScheduledThreadPoolExecutor(1);
        this.delayedCancellation = new ScheduledThreadPoolExecutor(1);
        this.task = task;
    }

    /**
     * Return an always variable between an interval TIMEOUT
     */
    private long getTIMEOUT(){
        return (long) BrokerSettings.getBrokerID() * APPEND_ENTRIES_TIME + ThreadLocalRandom.current().nextInt(3*APPEND_ENTRIES_TIME, 7*APPEND_ENTRIES_TIME + 1);//random timeout from 3 to 7 times more than append entries frequency
    }

    /**
     * Resets the timer but allows any running task to continue until expiring of the new task.
     */
    public synchronized void resetElectionTimer() {
        timer.getQueue().clear();
        timer.purge();

        var timeout = getTIMEOUT();

        if (BrokerSettings.getBrokerStatus() == BrokerStatus.Follower) {//clean completely executing and queued tasks.
            if (currentTaskThread.get() != null) {
                currentTaskThread.get().interrupt();
            }
        } else if (BrokerSettings.getBrokerStatus() == BrokerStatus.Candidate) {//Starting another election timer while an election is just started
            if (currentTaskThread.get() != null) {
                final Thread currentExecuting = currentTaskThread.get();
                //Schedule the current election to be cancelled if it fails to terminate (in case of tie)
                // to leave space on the pool for the new election
                delayedCancellation.schedule(currentExecuting::interrupt
                        ,timeout,TimeUnit.MILLISECONDS);
            }
        } else if (BrokerSettings.getBrokerStatus() == BrokerStatus.Leader) {//if I have become a leader i do not need to start another timer
            if (currentTaskThread.get() != null) {
                currentTaskThread.get().interrupt();
            }
            return;
        }

        // Schedule a new timer
        timer.schedule(
                () -> {
                    try {
                        // Store the currently running thread
                        currentTaskThread.set(Thread.currentThread());

                        //When starting an election you still have to start an election timer, it is started inside the task
                        // Run the task (which might block indefinitely on socket read)
                        task.run();
                    } finally {
                        // Clear flags when task completes (either normally or by interruption)
                        currentTaskThread.compareAndSet(Thread.currentThread(),null);
                    }
                },
                timeout,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * get the wait time in seconds
     */
    public long getWaitTimeForCurrentTimer(){
        var nextTimer = (ScheduledFuture<?>) timer.getQueue().peek();
        if(nextTimer != null){
            var returnVal = nextTimer.getDelay(TimeUnit.SECONDS);
            return returnVal > 0 ? returnVal : 0;
        }

        return 0;
    }

    /**
     * Cancels any scheduled task and shuts down the executor.
     */
    public void shutdown() {
        // Interrupt any running task
        if (currentTaskThread.get() != null) {
            currentTaskThread.get().interrupt();
        }

        timer.shutdown();
    }
}