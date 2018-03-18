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
    }


    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public int getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(int votedFor) {
        this.votedFor = votedFor;
    }

    public Log getLog() {
        return log;
    }



}
