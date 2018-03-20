package lib;

import java.io.Serializable;
/**
 * This class is a wrapper for packing all the result information that you
 * might use in your own implementation of the RequestVote call, and also
 * should be serializable to return by remote function call.
 *
 */
public class RequestVoteReply implements Serializable{
    static final long serialVersionUID = 42L;
    private int term;
    private boolean voteGranted;
    private static final long serialVersionUID = 3L;


    public RequestVoteReply(int term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int getTerm() {return term;}

    public void setTerm(int term) {this.term = term;}

    public boolean isVoteGranted() {return voteGranted;}

    public void setVoteGranted(boolean voteGranted) {this.voteGranted = voteGranted;}

}
