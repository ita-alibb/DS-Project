package it.distributedsystems;

import it.distributedsystems.raft.BrokerModel;
import it.distributedsystems.raft.BrokerState;
import it.distributedsystems.raft.LogLine;
import it.distributedsystems.raft.ReplicationLog;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

import static java.lang.System.exit;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReplicationLogTest extends ReplicationLog {

    @Test
    @DisplayName("Test replication")
    public void replicationTest(){
        var startIndex = 0;

        String FILE_HEADER = "Index;Epoch;JsonQueueCommand";

        if (startIndex < 0) { //indexes starts from 1
            //return new ArrayList<>();
        }

        //logUpdateLock.readLock().lock();
        try{
            //System.err.println("LOCK DEBUG: getLogsFromStartIndex acquired the READ lock");

            //try using cache:
            var searchingInts = IntStream.rangeClosed(startIndex, Math.min(1000, startIndex + 100)).boxed().toList();
            /*var cacheHit = cachedLogLines.stream().filter(ll -> searchingInts.contains(ll.getIndex()))
                    .sorted(Comparator.comparingInt(LogLine::getIndex)).toList();
            if (!cacheHit.isEmpty() && cacheHit.size() == searchingInts.size()) {//found all
                return cacheHit;
            }*/

            List<LogLine> logs = new ArrayList<>();

            //Cache miss:
            //Start reading backwards. Add on start
            try (ReversedLinesFileReader reader = new ReversedLinesFileReader(new File("C:/Users/aliba/Desktop/DS-Project/logs/2025-03-07/2.csv"), StandardCharsets.UTF_8)) {
                String line;

                while ((line = reader.readLine()) != null && !line.equals(FILE_HEADER)) {
                    var index = Integer.parseInt(line.split(";")[0]);

                    if (index >= startIndex) {
                        if (index <= startIndex + 100) logs.addFirst(new LogLine(line));
                    } else {
                        break;
                    }
                }
            } catch (IOException e) {
                System.err.println("Error while reading file: " + e.getMessage());
                exit(-1);
            }

            //return logs;
        } finally {
            //logUpdateLock.readLock().unlock();
            //System.err.println("LOCK DEBUG: getLogsFromStartIndex released the READ lock");
        }
    }

}
