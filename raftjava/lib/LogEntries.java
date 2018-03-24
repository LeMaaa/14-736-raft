/**
 * Created by lema on 2018/3/9.
 */
package lib;

import java.io.Serializable;

public class LogEntries implements Serializable{
    static final long serialVersionUID = 42L;
    private int term;
    private int index;
    private int command;
    
    public LogEntries(int term, int index, int command) {
        this.term = term;
        this.index = index;
        this.command = command;
    }

    // thread-safe access
    public synchronized int getTerm() {return this.term;}
    public synchronized int getIndex() {return  this.index;}
    public synchronized int getCommand() {return this.command;}

    public synchronized void setTerm(int term) {this.term = term;}
    public synchronized void setIndex(int index) {this.index = index;}
    public synchronized void setCommand(int command) {this.command = command;}

}
