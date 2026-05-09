package protocols.agreement.raft;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.AgreementProtocol;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.notifications.LeaderChangeNotification;
import protocols.agreement.raft.messages.AppendEntriesMessage;
import protocols.agreement.raft.messages.AppendEntriesReplyMessage;
import protocols.agreement.raft.messages.RequestVoteMessage;
import protocols.agreement.raft.messages.RequestVoteReplyMessage;
import protocols.agreement.raft.timers.ElectionTimer;
import protocols.agreement.raft.timers.HeartbeatTimer;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.statemachine.notifications.ChannelReadyNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class Raft extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(Raft.class);

    public static final short PROTOCOL_ID = AgreementProtocol.PROTOCOL_ID;
    public static final String PROTOCOL_NAME = "Raft";

    private static final long DEFAULT_HEARTBEAT_INTERVAL = 500;
    private static final long DEFAULT_ELECTION_TIMEOUT_MIN = 2000;
    private static final long DEFAULT_ELECTION_TIMEOUT_MAX = 6000;

    private Host myself;
    private List<Host> membership;
    private int channelId;

    private RaftRole role;

    private int currentTerm;
    private Host votedFor;
    private Host currentLeader;

    private final List<LogEntry> log;

    // highest log index known to be committed
    private int commitIndex;

    // highest log entry already delivered to the StateMachine
    private int lastApplied;

    // for leaders
    // The next AppendEntries to replicaX should start from log index y
    private final Map<Host, Integer> nextIndex;

    // highest log index known to be replicated on that follower
    private final Map<Host, Integer> matchIndex;

    // for election
    private final Set<Host> votesReceived;

    private long heartbeatInterval;
    private long electionTimeoutMin;
    private long electionTimeoutMax;

    private long electionTimerId;
    private long heartbeatTimerId;

    private final Random random;

    public Raft(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.membership = new LinkedList<>();
        this.role = RaftRole.FOLLOWER;

        this.currentTerm = 0;
        this.votedFor = null;
        this.currentLeader = null;

        this.log = new ArrayList<>();
        this.log.add(null); // index 0 unused, Raft log starts at 1

        this.commitIndex = 0;
        this.lastApplied = 0;

        this.nextIndex = new HashMap<>();
        this.matchIndex = new HashMap<>();
        this.votesReceived = new HashSet<>();

        this.heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
        this.electionTimeoutMin = DEFAULT_ELECTION_TIMEOUT_MIN;
        this.electionTimeoutMax = DEFAULT_ELECTION_TIMEOUT_MAX;

        this.electionTimerId = -1;
        this.heartbeatTimerId = -1;

        this.random = new Random();

        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        registerTimerHandler(ElectionTimer.TIMER_ID, this::uponElectionTimer);
        registerTimerHandler(HeartbeatTimer.TIMER_ID, this::uponHeartbeatTimer);

        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    @Override
    public void init(Properties props) {
        heartbeatInterval = Long.parseLong(
                props.getProperty("raft.heartbeat_interval", String.valueOf(DEFAULT_HEARTBEAT_INTERVAL)));

        electionTimeoutMin = Long.parseLong(
                props.getProperty("raft.election_timeout_min", String.valueOf(DEFAULT_ELECTION_TIMEOUT_MIN)));

        electionTimeoutMax = Long.parseLong(
                props.getProperty("raft.election_timeout_max", String.valueOf(DEFAULT_ELECTION_TIMEOUT_MAX)));

        if (electionTimeoutMax <= electionTimeoutMin) {
            throw new IllegalArgumentException(
                    "raft.election_timeout_max must be greater than raft.election_timeout_min");
        }

        logger.info(
                "Raft timers: heartbeat={}ms, electionTimeout=[{}, {}]ms",
                heartbeatInterval,
                electionTimeoutMin,
                electionTimeoutMax);
    }

    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        this.channelId = notification.getChannelId();
        this.myself = notification.getMyself();

        logger.info("Raft registered on shared channel {}, I am {}", channelId, myself);

        registerSharedChannel(channelId);

        registerMessageSerializer(channelId, RequestVoteMessage.MSG_ID, RequestVoteMessage.serializer);
        registerMessageSerializer(channelId, RequestVoteReplyMessage.MSG_ID, RequestVoteReplyMessage.serializer);
        registerMessageSerializer(channelId, AppendEntriesMessage.MSG_ID, AppendEntriesMessage.serializer);
        registerMessageSerializer(channelId, AppendEntriesReplyMessage.MSG_ID, AppendEntriesReplyMessage.serializer);

        try {
            registerMessageHandler(channelId, RequestVoteMessage.MSG_ID,
                    this::uponRequestVoteMessage, this::uponMsgFail);
            registerMessageHandler(channelId, RequestVoteReplyMessage.MSG_ID,
                    this::uponRequestVoteReplyMessage, this::uponMsgFail);
            registerMessageHandler(channelId, AppendEntriesMessage.MSG_ID,
                    this::uponAppendEntriesMessage, this::uponMsgFail);
            registerMessageHandler(channelId, AppendEntriesReplyMessage.MSG_ID,
                    this::uponAppendEntriesReplyMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering Raft message handlers.", e);
        }
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        this.membership = new LinkedList<>(notification.getMembership());

        logger.info("Raft joined. Membership: {}", membership);

        /*
         * No deterministic leader.
         * Every replica starts as FOLLOWER and waits for a randomized election timeout.
         * The first replica whose timeout expires becomes CANDIDATE and starts an
         * election.
         */
        role = RaftRole.FOLLOWER;
        currentLeader = null;
        votedFor = null;
        votesReceived.clear();

        resetElectionTimer();
    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        logger.debug("Raft received proposal {}", request);

        if (role != RaftRole.LEADER) {
            logger.warn("Ignoring proposal because I am not leader. Current leader is {}", currentLeader);
            return;
        }

        int index = log.size();
        LogEntry entry = new LogEntry(index, currentTerm, request.getOpId(), request.getOperation());
        log.add(entry);

        matchIndex.put(myself, index);

        for (Host h : membership) {
            sendToOtherAppendEntries(h);
        }

        /*
         * In a correct implementation, the entry is committed only after majority
         * replies.
         * This method tries to update commit based on matchIndex.
         */
        updateCommitIndex();
    }

    private void uponRequestVoteMessage(RequestVoteMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);

        boolean voteGranted = false;

        if (msg.getTerm() < currentTerm) {
            voteGranted = false;
        } else {
            if (msg.getTerm() > currentTerm) {
                becomeFollower(msg.getTerm(), null);
            }

            boolean notVotedYet = votedFor == null || votedFor.equals(from);

            // prevents a replica with an old log from becoming leader
            boolean candidateLogOk = isCandidateLogAtLeastAsUpToDate(
                    msg.getLastLogIndex(),
                    msg.getLastLogTerm());

            if (notVotedYet && candidateLogOk) {
                votedFor = from;
                voteGranted = true;

                /*
                 * After granting a vote, reset the election timer.
                 * This avoids starting another election immediately in the same term.
                 */
                resetElectionTimer();
            }
        }

        sendToOther(new RequestVoteReplyMessage(currentTerm, voteGranted), from);
    }

    private void uponRequestVoteReplyMessage(RequestVoteReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);

        if (role != RaftRole.CANDIDATE) {
            return;
        }

        if (msg.getTerm() > currentTerm) {
            becomeFollower(msg.getTerm(), null);
            return;
        }

        if (msg.getTerm() == currentTerm && msg.isVoteGranted()) {
            votesReceived.add(from);

            if (votesReceived.size() >= majority()) {
                becomeLeader();
                triggerNotification(new LeaderChangeNotification(myself));
            }
        }
    }

    private void uponAppendEntriesMessage(AppendEntriesMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);

        if (msg.getTerm() < currentTerm) {
            sendToOther(new AppendEntriesReplyMessage(currentTerm, false, lastLogIndex()), from);
            return;
        }

        if (msg.getTerm() >= currentTerm) {
            boolean leaderChanged = currentLeader == null || !currentLeader.equals(from);

            becomeFollower(msg.getTerm(), from);
            currentLeader = from;

            if (leaderChanged) {
                triggerNotification(new LeaderChangeNotification(from));
            }

            /*
             * AppendEntries is also the Raft heartbeat.
             * Any valid AppendEntries from the current leader resets the election timeout.
             */
            resetElectionTimer();
        }

        // log consistency check
        if (!logContainsEntry(msg.getPrevLogIndex(), msg.getPrevLogTerm())) {
            sendToOther(new AppendEntriesReplyMessage(currentTerm, false, lastLogIndex()), from);
            return;
        }

        // repair inconsistent logs
        for (LogEntry entry : msg.getEntries()) {
            if (entry.getIndex() < log.size()) {
                LogEntry existing = log.get(entry.getIndex());
                if (existing.getTerm() != entry.getTerm()) {
                    truncateLogFrom(entry.getIndex());
                    log.add(entry);
                }
            } else {
                log.add(entry);
            }
        }

        // learn the leader commit and apply them
        if (msg.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(msg.getLeaderCommit(), lastLogIndex());
            applyCommittedEntries();
        }

        sendToOther(new AppendEntriesReplyMessage(currentTerm, true, lastLogIndex()), from);
    }

    private void uponAppendEntriesReplyMessage(AppendEntriesReplyMessage msg, Host from, short sourceProto,
            int channelId) {
        logger.debug("Received {} from {}", msg, from);

        if (role != RaftRole.LEADER) {
            return;
        }

        if (msg.getTerm() > currentTerm) {
            becomeFollower(msg.getTerm(), null);
            return;
        }

        if (msg.isSuccess()) {
            matchIndex.put(from, msg.getMatchIndex());
            nextIndex.put(from, msg.getMatchIndex() + 1);
            updateCommitIndex();
        } else {
            int next = nextIndex.getOrDefault(from, lastLogIndex() + 1);
            nextIndex.put(from, Math.max(1, next - 1));
            sendToOtherAppendEntries(from);
        }
    }

    private void sendAppendEntries(Host h) {
        int next = nextIndex.getOrDefault(h, lastLogIndex() + 1);

        int prevLogIndex = next - 1;
        int prevLogTerm = termAt(prevLogIndex);

        List<LogEntry> entries = new ArrayList<>();
        for (int i = next; i < log.size(); i++) {
            entries.add(log.get(i));
        }

        AppendEntriesMessage msg = new AppendEntriesMessage(
                currentTerm,
                prevLogIndex,
                prevLogTerm,
                commitIndex,
                entries);

        sendToOther(msg, h);
    }

    private void becomeFollower(int term, Host leader) {
        boolean termChanged = term > currentTerm;

        role = RaftRole.FOLLOWER;
        currentTerm = term;
        currentLeader = leader;

        if (termChanged) {
            votedFor = null;
        }

        votesReceived.clear();

        cancelHeartbeatTimer();
        resetElectionTimer();

        logger.info("Became FOLLOWER term {}, leader {}", currentTerm, currentLeader);
    }

    private void becomeLeader() {
        role = RaftRole.LEADER;
        currentLeader = myself;

        if (electionTimerId != -1) {
            cancelTimer(electionTimerId);
            electionTimerId = -1;
        }

        nextIndex.clear();
        matchIndex.clear();

        int next = lastLogIndex() + 1;

        for (Host h : membership) {
            nextIndex.put(h, next);
            matchIndex.put(h, 0);
        }

        matchIndex.put(myself, lastLogIndex());

        logger.info("Became LEADER term {}", currentTerm);

        startHeartbeatTimer();

        for (Host h : membership) {
            sendToOtherAppendEntries(h);
        }
    }

    private void startElection() {
        role = RaftRole.CANDIDATE;
        currentTerm++;
        votedFor = myself;
        currentLeader = null;
        votesReceived.clear();
        votesReceived.add(myself);

        logger.info("Starting election for term {}", currentTerm);

        resetElectionTimer();

        RequestVoteMessage msg = new RequestVoteMessage(
                currentTerm,
                lastLogIndex(),
                termAt(lastLogIndex()));

        for (Host h : membership) {
            sendToOther(msg, h);
        }

        if (votesReceived.size() >= majority()) {
            becomeLeader();
            triggerNotification(new LeaderChangeNotification(myself));
        }
    }

    private void updateCommitIndex() {
        if (role != RaftRole.LEADER) {
            return;
        }

        for (int index = lastLogIndex(); index > commitIndex; index--) {
            int count = 0;

            for (Host h : membership) {
                int m = matchIndex.getOrDefault(h, 0);
                if (m >= index) {
                    count++;
                }
            }

            if (count >= majority() && termAt(index) == currentTerm) {
                commitIndex = index;
                applyCommittedEntries();

                for (Host h : membership) {
                    sendToOtherAppendEntries(h);
                }

                break;
            }
        }
    }

    // delivers committed log entries to the StateMachine in order
    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);

            triggerNotification(new DecidedNotification(
                    entry.getIndex(),
                    entry.getOpId(),
                    entry.getOperation()));
        }
    }

    private void uponElectionTimer(ElectionTimer timer, long timerId) {
        if (timerId != electionTimerId) {
            return;
        }

        if (role == RaftRole.LEADER) {
            return;
        }

        logger.warn("Election timeout expired. Current role {}, current leader {}", role, currentLeader);

        startElection();
    }

    private void uponHeartbeatTimer(HeartbeatTimer timer, long timerId) {
        if (timerId != heartbeatTimerId) {
            return;
        }

        if (role != RaftRole.LEADER) {
            return;
        }

        logger.debug("Sending heartbeat for term {}", currentTerm);

        for (Host h : membership) {
            sendToOtherAppendEntries(h);
        }
    }

    /*
     * -------------------------------------------------------------------------
     * Helpers
     * ----------------------------------------------------------------------
     */

    private void resetElectionTimer() {
        if (role == RaftRole.LEADER) {
            return;
        }

        if (electionTimerId != -1) {
            cancelTimer(electionTimerId);
        }

        electionTimerId = setupTimer(
                new ElectionTimer(),
                randomElectionTimeout());
    }

    private void startHeartbeatTimer() {
        if (heartbeatTimerId != -1) {
            cancelTimer(heartbeatTimerId);
        }

        heartbeatTimerId = setupPeriodicTimer(
                new HeartbeatTimer(),
                0,
                heartbeatInterval);
    }

    private void cancelHeartbeatTimer() {
        if (heartbeatTimerId != -1) {
            cancelTimer(heartbeatTimerId);
            heartbeatTimerId = -1;
        }
    }

    private long randomElectionTimeout() {
        long diff = electionTimeoutMax - electionTimeoutMin;
        return electionTimeoutMin + random.nextInt((int) diff);
    }

    private void sendToOtherAppendEntries(Host h) {
        if (h.equals(myself)) {
            return;
        }

        sendAppendEntries(h);
    }

    private void sendToOther(ProtoMessage msg, Host h) {
        if (h.equals(myself)) {
            return;
        }

        openConnection(h);
        sendMessage(msg, h);
    }

    private boolean logContainsEntry(int index, int term) {
        if (index == 0) {
            return true;
        }

        if (index >= log.size()) {
            return false;
        }

        return log.get(index).getTerm() == term;
    }

    private boolean isCandidateLogAtLeastAsUpToDate(int candidateLastIndex, int candidateLastTerm) {
        int myLastTerm = termAt(lastLogIndex());

        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }

        return candidateLastIndex >= lastLogIndex();
    }

    private void truncateLogFrom(int index) {
        while (log.size() > index) {
            log.remove(log.size() - 1);
        }
    }

    private int lastLogIndex() {
        return log.size() - 1;
    }

    private int termAt(int index) {
        if (index == 0) {
            return 0;
        }

        if (index >= log.size()) {
            return -1;
        }

        return log.get(index).getTerm();
    }

    private int majority() {
        return membership.size() / 2 + 1;
    }

    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received {}", request);
        membership.add(request.getReplica());
    }

    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received {}", request);
        membership.remove(request.getReplica());
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }
}