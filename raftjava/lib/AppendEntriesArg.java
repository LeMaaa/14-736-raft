/**
 * Created by lema on 2018/3/9.
 */
package lib;
import java.io.Serializable;
import java.util.*;

public class AppendEntriesArg implements Serializable {

    private int term;
    private int leaderId;
    private int prevLogIndex;
    private int prevLogTerm;
    private ArrayList<LogEntries> entries;
    private int leaderCommit;


    public AppendEntriesArg(int term, int leaderId, int prevLogIndex, int prevLogTerm, ArrayList<LogEntries> en, int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = en;
        this.leaderCommit = leaderCommit;
    }


    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(int prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public ArrayList<LogEntries> getEntries() {
        return entries;
    }

    public void setEntries(ArrayList<LogEntries> entries) {
        this.entries = entries;
    }

    public int getLeaderCommit() {return leaderCommit;}

    public void setLeaderCommit(int commitIndex) {
        this.leaderCommit = leaderCommit;
    }
}
