package lib;

import java.util.ArrayList;

/**
 * Created by lema on 2018/3/9.
 */
public class PersistentState {

    private int currentTerm;
    private int votedFor = -1;
    private Log log;

    public PersistentState() {
        log = new Log();
        // starting with term 1
        currentTerm = 1;
    }


    public synchronized int getCurrentTerm() {
        return currentTerm;
    }

    public synchronized void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public synchronized int getVotedFor() {
        return votedFor;
    }

    public synchronized void setVotedFor(int votedFor) {
        this.votedFor = votedFor;
    }

    public synchronized Log getLog() {
        return log;
    }



}
