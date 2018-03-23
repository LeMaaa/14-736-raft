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
        // placeholder for 0 index, so we don't need to convert
        // when accessing the log
        logs.add(null);
        commitIndex = -1;
        lastApplied = -1;
    }


    public synchronized void setCommitIndex(int val) {this.commitIndex = val;}
    public synchronized void setLastApplied(int val) {this.lastApplied = val;}
    public synchronized int getCommitIndex() {return commitIndex;}
    public synchronized int getLastApplied() {return lastApplied;}

    public void dumpEntries() {
        synchronized(logs) {
            System.out.println("========================================");
            for (int i = 0; i < logs.size(); i++) {
                if (i > 0)
                    System.out.println("Log Entry " + i + " contains " + 
                        logs.get(i).getCommand() + " with term: " + logs.get(i).getTerm());
            }
            System.out.println("========================================");
        }
    }
    // returns the last log index
    public synchronized int lastEntryIndex() {
        return this.lastIndex;
    }

    // returns the last log term or -1 if null
    public synchronized int lastEntryTerm() {
        if (this.lastEntryIndex() > 0) {
            return this.logs.get(lastIndex).getTerm();
        } else return -1;
    }

    public synchronized int getTermAtIndex(int index) {
        if (this.lastEntryIndex() <= index) {
            LogEntries l = this.logs.get(index);
            return l.getTerm();
        } else return -1;
    }

    // updates committed index
    public synchronized void updateCommitIndex(int index) {
        if (index > this.commitIndex) {
            this.commitIndex = (index < this.lastEntryIndex()? index : this.lastEntryIndex());
        }
    }

    public synchronized void deleteConflictingEntries(int index) {
        if(index < commitIndex) return;
        while (index <= lastIndex) {
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
                deleteConflictingEntries(entry.getIndex());
            } else {
                return true; // we already have this entry
            }
        }

        // validate that this is an acceptable entry to append next
        if (lastIndex == 0 || (entry.getIndex() == lastIndex + 1 && entry.getTerm() >= logs.get(lastIndex).getTerm())) {
            logs.add(entry);
            lastIndex = entry.getIndex();
            return true;
        }
        return false;
    }


    public synchronized LogEntries getEntry(int index) {
        if (logs == null || logs.size() == 0) return null;
        if (index < 0 || index > this.lastEntryIndex()) return null;
        return logs.get(index);
    }

    public synchronized ArrayList<LogEntries> getEntryFrom(int index) {
        if(index < 0 || index > lastEntryIndex()) return null;
        ArrayList<LogEntries> res = new ArrayList<>();
        for(int i = index; i <= lastEntryIndex(); i++) {
            res.add(logs.get(i));
        }
        return res;
    }

    public synchronized int getLastTerm() {
        // System.out.print("Current index:" + this.lastEntryIndex());
        if(lastEntryIndex() <= 0) return 0;
        return getEntry(lastIndex).getTerm();
    }
}
