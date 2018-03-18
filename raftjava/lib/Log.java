package lib;

import java.util.ArrayList;

/**
 * Created by lema on 2018/3/12.
 */
public class Log {

    private ArrayList<LogEntries> logs;
    private int commitIndex;
    private int lastApplied;

    private int lastIndex = 0;
    private int firstIndex = 0;


    public Log() {
        logs = new ArrayList<>();
        commitIndex = -1;
        lastApplied = -1;
    }

    public void setCommitIndex(int val) {this.commitIndex = val;}
    public void setLastApplied(int val) {this.lastApplied = val;}
    public int getCommitIndex() {return commitIndex;}
    public int getLastApplied() {return lastApplied;}


    // returns the last log index
    public int lastEntryIndex() {
        return this.lastIndex;
    }

    // returns the last log term or -1 if null
    public int lastEntryTerm() {
        if (this.lastEntryIndex() > 0) {
            return this.logs.get(lastIndex).getTerm();
        } else return -1;
    }

    public int getTermAtIndex(int index) {
        if (this.lastEntryIndex() <= index) {
            LogEntries l = (LogEntries) this.logs.get(index);
            return l.getTerm();
        } else return -1;
    }

    // updates committed index
    public void updateCommitIndex(int index) {
        if (index > this.commitIndex) {
            this.commitIndex = (index < this.lastEntryIndex()? index : this.lastEntryIndex());
        }
    }

    public void deleteConflictingEntries(int index) {
        if(index < commitIndex) return;
        while(index <= lastIndex) {
            this.logs.remove(lastIndex);
            lastIndex--;
        }
    }


    public synchronized boolean append(LogEntries entry) {
        assert entry != null;
        // check if the entry is already in our log
        if (entry.getIndex() <= lastIndex) {
            //assert entry.index >= commitIndex : entry.index + " >= " + commitIndex;
            if (entry.getTerm() != logs.get(entry.getIndex()).getTerm()) {
                deleteConflictingEntries(entry.getIndex();
            } else {
                return true; // we already have this entry
            }
        }

        // validate that this is an acceptable entry to append next
        if (entry.getIndex() == lastIndex + 1 && entry.getTerm() >= logs.get(lastIndex).getTerm()) {
            logs.add(entry);
            lastIndex = entry.getIndex();
            return true;
        }
        return false;
    }


    public LogEntries getEntry(int index) {
        if(index < 0 || index > lastEntryIndex()) return null;
        return logs.get(index);
    }

    public ArrayList<LogEntries> getEntryFrom(int index) {
        if(index < 0 || index > lastEntryIndex()) return null;
        ArrayList<LogEntries> res = new ArrayList<>();
        for(int i = index; i <= lastEntryIndex(); i++) {
            res.add(logs.get(i));
        }
        return res;
    }





}
