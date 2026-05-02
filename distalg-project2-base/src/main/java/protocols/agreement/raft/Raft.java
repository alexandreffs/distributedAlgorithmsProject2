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

    private Host myself;
    private List<Host> membership;
    private int channelId;

    private RaftRole role;

    private int currentTerm;
    private Host votedFor;
    private Host currentLeader;

    private final List<LogEntry> log;

    private int commitIndex;
    private int lastApplied;

    private final Map<Host, Integer> nextIndex;
    private final Map<Host, Integer> matchIndex;

    private final Set<Host> votesReceived;

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

        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    @Override
    public void init(Properties props) {
        // TODO: add election timeout and heartbeat timers.
        // For now, leader can be selected deterministically after JoinedNotification.
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
         * Temporary deterministic leader.
         * Replace this later with real randomized Raft elections.
         */
        Host first = membership.get(0);
        currentLeader = first;

        if (myself.equals(first)) {
            becomeLeader();
        } else {
            becomeFollower(currentTerm, first);
        }

        triggerNotification(new LeaderChangeNotification(currentLeader));
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
            if (!h.equals(myself)) {
                sendAppendEntries(h);
            }
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
            boolean candidateLogOk = isCandidateLogAtLeastAsUpToDate(
                    msg.getLastLogIndex(),
                    msg.getLastLogTerm());

            if (notVotedYet && candidateLogOk) {
                votedFor = from;
                voteGranted = true;
            }
        }

        sendMessage(new RequestVoteReplyMessage(currentTerm, voteGranted), from);
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
            sendMessage(new AppendEntriesReplyMessage(currentTerm, false, lastLogIndex()), from);
            return;
        }

        if (msg.getTerm() >= currentTerm) {
            becomeFollower(msg.getTerm(), from);
            currentLeader = from;
            triggerNotification(new LeaderChangeNotification(from));
        }

        if (!logContainsEntry(msg.getPrevLogIndex(), msg.getPrevLogTerm())) {
            sendMessage(new AppendEntriesReplyMessage(currentTerm, false, lastLogIndex()), from);
            return;
        }

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

        if (msg.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(msg.getLeaderCommit(), lastLogIndex());
            applyCommittedEntries();
        }

        sendMessage(new AppendEntriesReplyMessage(currentTerm, true, lastLogIndex()), from);
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
            sendAppendEntries(from);
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

        sendMessage(msg, h);
    }

    private void becomeFollower(int term, Host leader) {
        role = RaftRole.FOLLOWER;
        currentTerm = term;
        currentLeader = leader;
        votedFor = null;
        votesReceived.clear();

        logger.info("Became FOLLOWER term {}, leader {}", currentTerm, currentLeader);
    }

    private void becomeLeader() {
        role = RaftRole.LEADER;
        currentLeader = myself;

        nextIndex.clear();
        matchIndex.clear();

        int next = lastLogIndex() + 1;

        for (Host h : membership) {
            nextIndex.put(h, next);
            matchIndex.put(h, 0);
        }

        matchIndex.put(myself, lastLogIndex());

        logger.info("Became LEADER term {}", currentTerm);

        for (Host h : membership) {
            if (!h.equals(myself)) {
                sendAppendEntries(h);
            }
        }
    }

    private void startElection() {
        role = RaftRole.CANDIDATE;
        currentTerm++;
        votedFor = myself;
        votesReceived.clear();
        votesReceived.add(myself);

        RequestVoteMessage msg = new RequestVoteMessage(
                currentTerm,
                lastLogIndex(),
                termAt(lastLogIndex()));

        for (Host h : membership) {
            if (!h.equals(myself)) {
                sendMessage(msg, h);
            }
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
                    if (!h.equals(myself)) {
                        sendAppendEntries(h);
                    }
                }

                break;
            }
        }
    }

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