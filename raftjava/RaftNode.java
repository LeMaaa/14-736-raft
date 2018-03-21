
import lib.*;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.*;


public class RaftNode implements MessageHandling {
    private int id;
    private int leaderID;
    private static TransportLib lib;
    private int port;
    private int num_peers;
    private Types type;

    private LogEntries lastEntry;
    
    private long electionTimeout;
    private int electionTimeoutMills = 250;

    private double heartbeatMillis   = 250;

    private PersistentState state;
    private static final Random random = new Random();
    private static final long T = 150;  //100ms

    private int commitIndex = 0; // index of highest log entry known to be committed
    private int lastApplied = 0; //  index of highest log entry applied to state machine
    private int firstIndexOfTerm = 0;

    private ArrayList<Integer> nextIndex; // for each server, index of the next log entry to sent to that server
    private ArrayList<Integer> matchIndex; //  for each server, index of highest log entry known to be replicated on server


    private void resetElectionTimeout(){
        electionTimeout = T + System.currentTimeMillis() + T + random.nextInt(electionTimeoutMills);

    }

    public int getPort() {return this.port;}
    public void setLeaderID(int id) {this.leaderID = id;}

    public RaftNode(int port, int id, int num_peers) throws Exception {
        this.id = id;
        this.port = port;
        this.num_peers = num_peers;
        this.leaderID = -1;

        lib = new TransportLib(port, id, this);

        this.state = new PersistentState();

        nextIndex = new ArrayList<>();
        matchIndex = new ArrayList<>();
        try {
            // set election timeout before launching periodic tasks
            resetElectionTimeout();
            launchPeriodicTasksThread();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("RaftNode created!");
    }


    // respond to vote request from peers
    public synchronized RequestVoteReply requestVote(RequestVoteArgs requestVoteArgs) {
        if(requestVoteArgs.getTerm() < state.getCurrentTerm()) {
            return new RequestVoteReply(state.getCurrentTerm(), false);
        }

        boolean voted = false;

        if(requestVoteArgs.getTerm() > state.getCurrentTerm()) {
            if(this.type != Types.FOLLOWER) {
                this.toFollower(requestVoteArgs.getTerm(), requestVoteArgs.getCandidateId());
            }
        }

        if(requestVoteArgs.getTerm() == this.state.getCurrentTerm() && (state.getVotedFor() == -1
                || state.getVotedFor() == requestVoteArgs.getCandidateId())
                &&  (requestVoteArgs.getLastLogIndex() >= this.lastEntry.getIndex()
                || requestVoteArgs.getTerm() > this.lastEntry.getTerm())) {

            voted = true;
            this.state.setVotedFor(requestVoteArgs.getCandidateId());
            resetElectionTimeout();

        }
        return new RequestVoteReply(state.getCurrentTerm(), voted);
    }


    // send appendEntriesRequest Handler
    // reply to hearbeats and log entries
    public synchronized AppendEntriesReply AppendEntries(AppendEntriesArg appendEntriesArg) {
        if(appendEntriesArg.getTerm() < state.getCurrentTerm()) {
            return new AppendEntriesReply(state.getCurrentTerm(), false);
        }

        // transfer to follower status if the current node is waiting for election result
        if (appendEntriesArg.getTerm() > state.getCurrentTerm()) {
            this.state.setCurrentTerm(appendEntriesArg.getTerm());
            this.state.setVotedFor(appendEntriesArg.getLeaderId());
            this.type = Types.FOLLOWER;
        }

        resetElectionTimeout();

        // consistency check;

        AppendEntriesReply res = new AppendEntriesReply(this.state.getCurrentTerm(), false);
        boolean isConsistent = false;

        if (this.state.getLog().lastEntryIndex() < appendEntriesArg.getPrevLogIndex()
                || appendEntriesArg.getPrevLogTerm() != this.state.getLog().getEntry(appendEntriesArg.getPrevLogIndex()).getTerm()) {
            isConsistent = false;
        }else {
            isConsistent = true;
        }

        if(isConsistent) {
            if (appendEntriesArg.getEntries() != null || appendEntriesArg.getEntries().size() != 0) {
                for (LogEntries e : appendEntriesArg.getEntries()) {
                    if (!state.getLog().append(e)) {
                        return new AppendEntriesReply(this.state.getCurrentTerm(), false);
                    }
                }
            }
            state.getLog().setCommitIndex(Math.min(appendEntriesArg.getLeaderCommit(), state.getLog().lastEntryIndex()));
            return new AppendEntriesReply(this.state.getCurrentTerm(), true);
        }else {
            if(appendEntriesArg.getPrevLogIndex() > commitIndex) {
                this.state.getLog().deleteConflictingEntries(appendEntriesArg.getPrevLogIndex());
            }
            return new AppendEntriesReply(this.state.getCurrentTerm(), false);
        }
    }



    // run periodic task: election and hearbeat
    // election will only be run if there is no leader in the cluster
    // or when the leader doesn't respond
    private void launchPeriodicTasksThread() {
        final Thread t = new Thread(() -> {
                try {
                    while(true) {
                        runPeriodicTasks();
                        Thread.sleep(100);
                    }
                } catch (Throwable e) {
                   e.printStackTrace();
                }

        }, "RaftNode");
        t.start();
    }


    // periodic task, for now just choose new leader
    private synchronized void runPeriodicTasks() throws Exception {
        // If currently we have no leader
        if (this.leaderID == -1) {
            System.out.println("Current we have no leader, so let's start election.");
            // select a leader

            if(System.currentTimeMillis() > electionTimeout) {
                startElection();
            }
        } 
    }



    public synchronized void startElectionProcess() throws RemoteException,IOException,ClassNotFoundException {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {

            public void run() {

                if(electionTimeout == 0.0) {
                    if (type != Types.LEADER) {
                        try {
                            startElection();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }, 0, 100);
    }

    // rewrite the start election logic

    public synchronized void startElection() throws RemoteException, IOException, ClassNotFoundException {

        System.out.println("Starting election...");

        // transition to candidate
        this.toCandidate();

        int votes = 1;

        int lastIndex = state.getLog().lastEntryIndex();
        System.out.println(lastIndex);
        int lastTerm = state.getLog().getLastTerm();

        RequestVoteArgs ra = new RequestVoteArgs(this.state.getCurrentTerm(), this.id, lastIndex, lastTerm);
        byte[] data = SerializationUtils.toByteArray(ra);

        if(num_peers > 1) {
            for(int i = 0; i < this.num_peers; i++) {
                if(i == id) continue;
                Message msg = new Message(MessageType.RequestVoteArgs, id, i ,data);
                Message cur = lib.sendMessage(msg);
                RequestVoteReply reply = (RequestVoteReply) SerializationUtils.toObject(cur.getBody());

                if (reply.getTerm() > this.state.getCurrentTerm()) {
                    this.toFollower(reply.getTerm(), i);
                    break;
                }else if(reply.getTerm() == this.state.getCurrentTerm() ){
                    if(reply.isVoteGranted()) {
                        votes++;
                    }
                    if(votes > num_peers/2) {
                        if(this.type == Types.CANDIDATE && ra.getTerm() == this.state.getCurrentTerm()) {
                            toLeader();
                        }else {
                            break;
                        }
                    }
                }
            }
        } else {
            this.toLeader();
        }
    }

    public synchronized void toCandidate() {
        this.state.setCurrentTerm(this.state.getCurrentTerm() + 1);
        this.state.setVotedFor(this.id);
        this.type = Types.CANDIDATE;

        System.out.println("to Candidate Status");

    }


    // broadcast to all the peers that current node is the leader
    public synchronized void broadcastTo() {
        assert type == Types.LEADER;
        for(int i = 0; i < num_peers; i++) {
            try {
                sendAppendEntriesRequest(i);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public synchronized void toLeader() {
        nextIndex.clear();
        matchIndex.clear();
        this.type = Types.LEADER;
        leaderID = id;
        firstIndexOfTerm = this.state.getLog().lastEntryIndex() + 1;
        for (int i = 1; i <= num_peers; i++) {
            matchIndex.add(i,0);
            nextIndex.add(i, firstIndexOfTerm);
            assert nextIndex.get(i) != 0;
        }
        broadcastTo();
    }

    public synchronized void toFollower(int term, int leaderId) {
        this.type = Types.FOLLOWER;
        this.state.setCurrentTerm(term);
        this.state.setVotedFor(leaderId);

        System.out.println("To Follower Status");
    }

    public synchronized void sendAppendEntriesRequest(int serverId)
            throws RemoteException, ClassNotFoundException, IOException{

        if(type != Types.LEADER) return;
        ArrayList<LogEntries> entries = new ArrayList<LogEntries>();
        if(this.state.getLog().lastEntryIndex() >= nextIndex.get(serverId)) {
            entries = state.getLog().getEntryFrom(nextIndex.get(serverId));
        }
        AppendEntriesArg args = new AppendEntriesArg(this.state.getCurrentTerm(),
                this.id, nextIndex.get(serverId) - 1, state.getLog().getEntry(nextIndex.get(serverId)).getTerm(),
                entries, commitIndex);

        Message msg = new Message(MessageType.AppendEntriesArg, id, serverId, SerializationUtils.toByteArray(args));
        Message re = lib.sendMessage(msg);

        AppendEntriesReply res = (AppendEntriesReply) SerializationUtils.toObject(re.getBody());

        if(res.getTerm() > state.getCurrentTerm()) {
            toFollower(res.getTerm(), serverId);
        } else {
            if(res.isSuccess()) {
                if (entries == null || entries.size() == 0) {
                    nextIndex.set(serverId,  Math.max(state.getLog().lastEntryIndex() + 1, 1));
                }else {
                    matchIndex.set(serverId, state.getLog().lastEntryIndex());
                    nextIndex.set(serverId, matchIndex.get(serverId) + 1);
                }
            }else {
                // fail because of log inconsistency, then decrement nextIndex and retry
                if (nextIndex.get(serverId) > state.getLog().lastEntryIndex()) {
                    nextIndex.set(serverId,Math.max(state.getLog().lastEntryIndex() + 1, 1));
                } else if (nextIndex.get(serverId) > 1) {
                    nextIndex.set(serverId, nextIndex.get(serverId) - 1);
                }
            }
        }
        commitEntry();
    }


    public synchronized void commitEntry() throws RemoteException {
        if(type != Types.LEADER) return;
        if(!isCommittable(firstIndexOfTerm)) return;
        ArrayList<Integer> copy = new ArrayList<>(matchIndex);
        Collections.sort(copy);

        int newCommitIndex = copy.get(num_peers/2);

        if (state.getLog().getEntry(newCommitIndex).getTerm() != state.getCurrentTerm()) {
            return;
        }
        if (commitIndex >= newCommitIndex) {
            return;
        }

        int preCommitIndex = commitIndex;
        commitIndex = newCommitIndex;

        for(int i = preCommitIndex + 1; i <= commitIndex; i++) {
            ApplyMsg msg = new ApplyMsg(id, i, state.getLog().getEntry(i).getCommand(), false, null);
            lib.applyChannel(msg);
        }
        lastApplied = commitIndex;
    }

    public synchronized boolean isCommittable(int index) {
        int majority = 1 + num_peers/2;
        int cnt = 0;

        for(int i = 0; i < num_peers; i++) {
            if(matchIndex.get(i) >= index) {
                cnt++;
            }
            if(cnt >= majority) return true;
        }
        return cnt >= majority;
    }

    @Override
    public StartReply start(int command) {
        int term = this.state.getCurrentTerm();
        int index = -1;
        boolean isLeader = this.type == Types.LEADER;

        if(!isLeader) {
            return new StartReply(index, term, false);
        }

        for(int i = commitIndex + 1; i < this.state.getLog().lastEntryIndex(); i++ ) {
            if (state.getLog().getEntry(i).getCommand() == command){
                state.getLog().getEntry(i).setTerm(term);
                return new StartReply(index, term, true);
            }
        }

        index = this.state.getLog().lastEntryIndex() + 1;
        LogEntries entry = new LogEntries(term, index, command);
        this.state.getLog().append(entry);

        for(int i = 0; i < num_peers; i++) {
            if(i == id) continue;
            try {
                sendAppendEntriesRequest(i);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        StartReply sr = new StartReply(state.getLog().lastEntryIndex(), term, true);
        return sr;
    }

    @Override
    public GetStateReply getState() {
        GetStateReply gr = new GetStateReply(this.state.getCurrentTerm(), this.type == Types.LEADER);
        return gr;
    }

    @Override
    public Message deliverMessage(Message message) {

        if(message == null || message.getType() == null || message.getBody() == null
                || message.getDest() != id || message.getType() == MessageType.AppendEntriesReply
                || message.getType() == MessageType.AppendEntriesReply) return null;

        if(message.getType() == MessageType.RequestVoteArgs) {
            RequestVoteArgs cur = null;
            try {
                cur = (RequestVoteArgs) SerializationUtils.toObject(message.getBody());
            }catch (Exception e) {
                e.printStackTrace();
            }

            RequestVoteReply res  = this.requestVote(cur);
            byte[] data = null;
            try {
                 data = SerializationUtils.toByteArray(res);
            }catch (Exception e) {
                e.printStackTrace();
            }
            Message reply = new Message(MessageType.RequestVoteReply, id, message.getSrc(), data);
            return reply;
        }else if(message.getType() == MessageType.AppendEntriesArg) {
            AppendEntriesArg aa = null;
            try {
                aa = (AppendEntriesArg) SerializationUtils.toObject(message.getBody());
            }catch (Exception e) {
                e.printStackTrace();
            }
            AppendEntriesReply ar = this.AppendEntries(aa);
                byte[] data = null;
            try {
                data = SerializationUtils.toByteArray(ar);
            }catch (Exception ee) {
                ee.printStackTrace();
            }
            Message reply = new Message(MessageType.AppendEntriesReply, id, message.getSrc(), data);
            return reply;
        }else {
            return null;
        }
    }

    //main function
    public static void main(String args[]) throws Exception {
        if (args.length != 3) throw new Exception("Need 2 args: <port> <id> <num_peers>");
        //new usernode
        RaftNode UN = new RaftNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
    }
}
