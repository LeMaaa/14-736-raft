
import lib.*;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class RaftNode implements MessageHandling {
    private int id;
    private int leaderID;
    private static TransportLib lib;
    private int port;
    private int num_peers;
    private Types type;

    private LogEntries lastEntry;
    
    private long electionTimeout;
    private int electionTimeoutMults = 20;

    private double heartbeatMillis   = 250;

    private PersistentState state;
    private static final Random random = new Random();

    // base electio ntimeout
    private static final long T = 200;  //100ms

    private int commitIndex = 0; // index of highest log entry known to be committed
    private int lastApplied = 0; //  index of highest log entry applied to state machine
    private int firstIndexOfTerm = 0;

    private ArrayList<Integer> nextIndex; // for each server, index of the next log entry to sent to that server
    private ArrayList<Integer> matchIndex; //  for each server, index of highest log entry known to be replicated on server


    private synchronized void resetElectionTimeout() {
        // electionTimeout: 200 to 350ms
        electionTimeout = System.currentTimeMillis() + T + random.nextInt(electionTimeoutMults)*10;

    }

    public int getPort() {return this.port;}
    public void setLeaderID(int id) {this.leaderID = id;}

    public RaftNode(int port, int id, int num_peers) throws Exception {
        this.id = id;
        this.port = port;
        this.num_peers = num_peers;
        this.leaderID = -1;

        // start as follower
        this.type = Types.FOLLOWER;

        if (lib == null)
            lib = new TransportLib(port, id, this);
        else
            System.out.println("lib is already defined!!!");

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

        System.out.println("\nRaftNode created with id: " + id + " starting at port " + port + "\n");
    }


    // respond to vote request from peers
    public synchronized RequestVoteReply requestVote(RequestVoteArgs requestVoteArgs) {
        // if request vote args has smaller term
        // vote false, since the leader should be the most updated
        // if(requestVoteArgs.getTerm() < state.getCurrentTerm()) {
        //     return new RequestVoteReply(state.getCurrentTerm(), false);
        // }

        System.out.println("Got a vote request from instance " + requestVoteArgs.getCandidateId());
        boolean voted = false;

        // a valid vote should have the same term (updated above)
        // should have votedFor set for this candidate or unset
        // should have more updated last log index

        // || requestVoteArgs.getTerm() > this.lastEntry.getTerm())

        if(requestVoteArgs.getTerm() >= this.state.getCurrentTerm() && (state.getVotedFor() == -1
                || state.getVotedFor() == requestVoteArgs.getCandidateId())
                &&  (this.lastEntry == null || requestVoteArgs.getLastLogIndex() >= this.lastEntry.getIndex())) {

            // set current as follower since others have higher term
            if (requestVoteArgs.getTerm() > this.state.getCurrentTerm())
                this.toFollower(requestVoteArgs.getTerm(), requestVoteArgs.getCandidateId());

            voted = true;
            this.state.setVotedFor(requestVoteArgs.getCandidateId());
            resetElectionTimeout();

        }


        return new RequestVoteReply(state.getCurrentTerm(), voted);
    }


    // handle append entries request
    // reply to hearbeats and log entries
    public synchronized AppendEntriesReply AppendEntries(AppendEntriesArg appendEntriesArg) {

        // System.out.println("Received append entries request...");

        if(appendEntriesArg.getTerm() < state.getCurrentTerm()) {
            return new AppendEntriesReply(state.getCurrentTerm(), false);
        }

        // transfer to follower status if the current node is waiting for election result
        // also update the current leader
        if (appendEntriesArg.getTerm() > state.getCurrentTerm()) {
            this.state.setCurrentTerm(appendEntriesArg.getTerm());
            this.state.setVotedFor(appendEntriesArg.getLeaderId());
            this.leaderID = appendEntriesArg.getLeaderId();
            this.type = Types.FOLLOWER;
        }

        resetElectionTimeout();

        // if this is a heartbeat, simply reply success
        if (appendEntriesArg.getEntries() == null) {
            System.out.println("Received heartbeat, reply true");
            return new AppendEntriesReply(this.state.getCurrentTerm(), true);
        }

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
        } else {
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
                        Thread.sleep(100);
                        runPeriodicTasks();
                    }
                } catch (Throwable e) {
                   e.printStackTrace();
                }

        }, "RaftNode");
        t.start();
    }


    // periodic task, for now just choose new leader
    // don't have to be synchronized
    private void runPeriodicTasks() throws Exception {
        // run election as long as we don't receive heartbeat from leader

        if(System.currentTimeMillis() > electionTimeout && (type != Types.LEADER)) {
            startElection();
        }

        // if current node is leader, periodically send heartbeat
        if (type == Types.LEADER) {
            for (int i = 0; i < num_peers; i++) {
                if (i != id)
                    sendHeartbeatToServer(i);
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

    // election probably does not have to be synchronized

    public void startElection() throws RemoteException, IOException, ClassNotFoundException {

        System.out.println("Starting election at node " + this.id);

        // transition to candidate
        this.toCandidate();

        // reset election timeout since election is started
        resetElectionTimeout();

        AtomicInteger votes = new AtomicInteger(1);

        int lastIndex = state.getLog().lastEntryIndex();
        int lastTerm = state.getLog().getLastTerm();

        // prepare vote request
        RequestVoteArgs ra = new RequestVoteArgs(this.state.getCurrentTerm(), this.id, lastIndex, lastTerm);
        byte[] data = SerializationUtils.toByteArray(ra);

        if (num_peers > 1) {
            // System.out.println("We have " + num_peers + " peers.");

            for(int i = 0; i < this.num_peers; i++) {
                if(i == id) continue;

                // send message to corresponding node
                System.out.println("Trying to send request vote");

                // need to persistently try until got a message
                Message msg = new Message(MessageType.RequestVoteArgs, id, i, data);
                System.out.println("Request vote sent to node " + i);

                Message cur = null;
                RequestVoteReply reply = null;

                // keep sending until got reply
                // while (cur == null) {
                //     System.out.println("Trying to get a good reply...");
                //     synchronized(lib) {
                        cur = lib.sendMessage(msg);
                        if (cur == null)
                            continue;
                //     }
                // }
                System.out.println("Got reply for message!");

                reply = (RequestVoteReply) SerializationUtils.toObject(cur.getBody());

                System.out.println("Got vote reply!");
                System.out.println("Reply term: " + reply.getTerm() + " Vote: " + reply.isVoteGranted());
                if (reply.getTerm() > this.state.getCurrentTerm()) {
                    // reply has higher term, means current node cannot be leader
                    this.toFollower(reply.getTerm(), i);
                    break;
                } else if (reply.getTerm() <= this.state.getCurrentTerm()) {

                    System.out.println("Valid vote!");
                    if (reply.isVoteGranted()) {
                        votes.getAndIncrement();
                    }

                    // more than half, selected as leader
                    if(votes.get() > num_peers/2) {
                        System.out.println("More than half of votes received!");
                        System.out.println("The current type is " + this.type);
                        if (this.type == Types.CANDIDATE) {
                            toLeader();
                        } else {

                            // was set as follower in the middle, so we give up
                            break;
                        }
                    }
                }
            }
        } else {
            System.out.println("We have no peers.");
            this.toLeader();
        }

        System.out.println("Election finishes, the leader is: " + leaderID);
    }

    // Upon wining election, send heartbeats to server
    public void broadcastTo() {
        assert type == Types.LEADER;
        for(int i = 0; i < num_peers; i++) {
            try {
                if (i != id)
                    sendHeartbeatToServer(i);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    // update term and set voted for, and convert the type
    public synchronized void toCandidate() {
        this.state.setCurrentTerm(this.state.getCurrentTerm() + 1);
        this.state.setVotedFor(this.id);
        this.type = Types.CANDIDATE;

        System.out.println("to Candidate Status");
    }


    public synchronized void toLeader() {
        System.out.println("Converting to leader");
        nextIndex.clear();
        matchIndex.clear();
        this.type = Types.LEADER;
        leaderID = id;
        firstIndexOfTerm = this.state.getLog().lastEntryIndex() + 1;
        // reinitialize matchIndex and nextIndex
        for (int i = 0; i < num_peers; i++) {
            matchIndex.add(0);
            nextIndex.add(firstIndexOfTerm);
            assert nextIndex.get(i) != 0;
        }
        broadcastTo();
    }

    // convert current node state to follower
    // update current term
    // and update votedFor state

    public synchronized void toFollower(int term, int leaderId) {
        this.state.setCurrentTerm(term);
        this.state.setVotedFor(leaderId);
        this.type = Types.FOLLOWER;

        System.out.println("To Follower Status");
    }

    public synchronized void sendHeartbeatToServer(int serverId) 
            throws RemoteException, ClassNotFoundException, IOException {
        System.out.println("Sending Heartbeat to server: " + serverId);

        if(type != Types.LEADER) return;

        // empty entries
        // will not update logs

        // build append entries argument
        // current term, leaderId, prevLogIndex, prevTerm, entries, commitIndex
        // be careful with the messages...
        AppendEntriesArg args = new AppendEntriesArg(this.state.getCurrentTerm(),
                this.id, 0, 0, null, commitIndex);

        Message msg = new Message(MessageType.AppendEntriesArg, id, serverId, SerializationUtils.toByteArray(args));

        Message re = null;
        AppendEntriesReply res = null;

        // while (re == null) {
        //     System.out.println("Trying to get a good heartbeat reply.");
        //     synchronized(lib) {
                re = lib.sendMessage(msg);
                if (re == null) {
                    return;
                }
        //     }
        // }

        res = (AppendEntriesReply) SerializationUtils.toObject(re.getBody());


        if(res.getTerm() > state.getCurrentTerm()) {
            // response has higher term
            // reset current to follower
            // wait for another round of election to start
            toFollower(res.getTerm(), serverId);
        }


    }

    // used to send heart beat message,
    // log entry message, and leader change message
    public synchronized void sendAppendEntriesRequest(int serverId)
            throws RemoteException, ClassNotFoundException, IOException{
        System.out.println("Sending Append Entries Request to server: " + serverId);
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

    // parse message
    @Override
    public Message deliverMessage(Message message) {

        if (message == null || message.getType() == null || message.getBody() == null
                || message.getDest() != id || message.getType() == MessageType.RequestVoteReply
                || message.getType() == MessageType.AppendEntriesReply) {

            System.out.println("invalid message");
            return null;
        }

        if (message.getType() == MessageType.RequestVoteArgs) {
            RequestVoteArgs cur = null;
            try {
                cur = (RequestVoteArgs) SerializationUtils.toObject(message.getBody());
            } catch (Exception e) {
                e.printStackTrace();
            }

            // System.out.println("Before entering synchronized method");
            RequestVoteReply res  = this.requestVote(cur);
            // System.out.println("No deadlock yay!");

            byte[] data = null;
            try {
                 data = SerializationUtils.toByteArray(res);
            } catch (Exception e) {
                e.printStackTrace();
            }

            Message reply = new Message(MessageType.RequestVoteReply, id, message.getSrc(), data);

            // System.out.println("RequestVoteReply should be delivered");
            return reply;
        } else if (message.getType() == MessageType.AppendEntriesArg) {

            AppendEntriesArg aa = null;
            try {
                aa = (AppendEntriesArg) SerializationUtils.toObject(message.getBody());
            } catch (Exception e) {
                e.printStackTrace();
            }

            // System.out.println("Before entering synchronized method");
            AppendEntriesReply ar = this.AppendEntries(aa);
            // System.out.println("No deadlock yay!");

            byte[] data = null;

            try {
                data = SerializationUtils.toByteArray(ar);
            } catch (Exception e) {
                e.printStackTrace();
            }

            Message reply = new Message(MessageType.AppendEntriesReply, id, message.getSrc(), data);

            // System.out.println("AppendEntriesReply should be delivered");

            return reply;
        } else {
            System.out.println("Invalid message type, returning NULL");
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
