package it.distributedsystems;

import it.distributedsystems.raft.BrokerModel;
import it.distributedsystems.raft.BrokerState;
import it.distributedsystems.raft.ReplicationLog;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReplicationLogTest extends ReplicationLog {
/*
    @BeforeAll
    public static void initialize(){
        LOG_FILE_PATH = "src/test/java/it/distributedsystems/testLog.csv";
    }

    @Test
    @DisplayName("Test replication")
    public void replicationTest(){
        BrokerState.setCommitIndex(2);

        try {
            Thread.sleep(5_000);
        } catch (InterruptedException e) {
            assert(false);
        }

        var queue = BrokerModel.getInstance().getQueues();

        assertTrue(queue.containsKey("queueClient0"));
        assertTrue(queue.containsKey("queueClient1"));
    }*/
}
