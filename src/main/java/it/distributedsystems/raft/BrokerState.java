package it.distributedsystems.raft;

/**
 * Persistentstateonallservers:
 *  (UpdatedonstablestoragebeforerespondingtoRPCs)
 *  currentTerm latesttermserverhasseen(initializedto0
 *  onfirstboot,increasesmonotonically)
 *  votedFor candidateIdthatreceivedvoteincurrent
 *  term(ornullifnone)
 *  log[] logentries;eachentrycontainscommand
 *  forstatemachine,andtermwhenentry
 *  wasreceivedbyleader(firstindexis1)
 *  Volatilestateonallservers:
 *  commitIndex indexofhighestlogentryknowntobe
 *  committed(initializedto0,increases
 *  monotonically)
 *  lastApplied indexofhighestlogentryappliedtostate
 *  machine(initializedto0,increases
 *  monotonically)
 *  Volatilestateonleaders:
 *  (Reinitializedafterelection)
 *  nextIndex[] foreachserver,indexofthenextlogentry
 *  tosendtothatserver(initializedtoleader
 *  lastlogindex+1)
 *  matchIndex[] foreachserver,indexofhighestlogentry
 *  knowntobereplicatedonserver
 *  (initializedto0,increasesmonotonically)
 */
public class BrokerState {
    //region Persistent state on all servers (Updated on stable storage before responding to RPCs):
    private static int currentTerm = 0;
    private static Integer votedFor = null;
    // log is handled in ReplicationLog
    //endregion

    //region Volatile state on all servers:
    private static int commitIndex = 0;
    private static int lastApplied = 0; //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    //endregion

    public synchronized static int getCurrentTerm() {
        return currentTerm;
    }

    public synchronized static void setCurrentTerm(int currentTerm) {
        if (currentTerm == BrokerState.currentTerm) return;
        if (currentTerm < BrokerState.currentTerm) {
            System.out.println("Error, set currentTerm should be monotonically increasing!");
            return;
        }
        //If the current term is different, update also voted for as null, means that the current term went up
        setVotedFor(null);

        BrokerState.currentTerm = currentTerm;

        ReplicationLog.storePersistentState(BrokerState.serialize());
    }

    public synchronized static Integer getVotedFor() {
        return votedFor;
    }

    public synchronized static void setVotedFor(Integer votedFor) {
        BrokerState.votedFor = votedFor;
        if (votedFor != null) {
            ReplicationLog.storePersistentState(BrokerState.serialize());
        }
    }

    public synchronized static int getCommitIndex() {
        return BrokerState.commitIndex;
    }

    public synchronized static void setCommitIndex(int commitIndex) {
        if (commitIndex < BrokerState.commitIndex) {
            System.out.println("Error, set commitIndex should be monotonically increasing!");
            return;
        } else if (commitIndex == BrokerState.commitIndex) {
            //it is not a new commit index, it is the same
            if (BrokerState.lastApplied == BrokerState.commitIndex) {//if the last applied is already at commit index return
                return;
            }
        }

        BrokerState.commitIndex = commitIndex;
        //If you update commit index you have to apply it to the model
        ReplicationLog.applyReplicationLogAsync();
    }

    public synchronized static int getLastApplied() {
        return lastApplied;
    }

    public synchronized static void setLastApplied(int lastApplied) {
        if (lastApplied < BrokerState.lastApplied) {
            System.out.println("Error, set lastApplied should be monotonically increasing!");
            return;
        }
        BrokerState.lastApplied = lastApplied;
    }

    private synchronized static String serialize(){
        return String.format("currentTerm=%d;votedFor=%d", currentTerm, votedFor);
    }

    public synchronized static void reloadPersistentState(String serialized){
        if (serialized.isEmpty()) return;

        serialized= serialized.replace("currentTerm=","");
        serialized= serialized.replace("votedFor=","");

        var match = serialized.split(";");
        currentTerm = Integer.parseInt(match[0]);
        if (match.length > 1 && !match[1].isEmpty()) {
            votedFor = (match[1].equals("null")) ? null : Integer.parseInt(match[1]);
        }
    }
}
