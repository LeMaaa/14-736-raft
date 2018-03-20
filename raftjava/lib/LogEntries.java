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
    private static final long serialVersionUID = 1L;
    
    public LogEntries(int term, int index, int command) {
        this.term = term;
        this.index = index;
        this.command = command;
    }

    public int getTerm() {return this.term;}
    public int getIndex() {return  this.index;}
    public int getCommand() {return this.command;}

    public void setTerm(int term) {this.term = term;}
    public void setIndex(int index) {this.index = index;}
    public void setCommand(int command) {this.command = command;}

}
