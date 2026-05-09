package protocols.agreement.multipaxos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.AgreementProtocol;
import protocols.agreement.multipaxos.messages.*;
import protocols.agreement.multipaxos.timers.HeartbeatTimer;
import protocols.agreement.multipaxos.timers.LeaderTimeoutTimer;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.notifications.LeaderChangeNotification;
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

public class MultiPaxos extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(MultiPaxos.class);

    public static final short PROTOCOL_ID = AgreementProtocol.PROTOCOL_ID;
    public static final String PROTOCOL_NAME = "MultiPaxos";

    private static final long DEFAULT_HEARTBEAT_INTERVAL = 500;
    private static final long DEFAULT_ELECTION_TIMEOUT_MIN = 2000;
    private static final long DEFAULT_ELECTION_TIMEOUT_MAX = 6000;

    private Host myself;
    private List<Host> membership;
    private int channelId;

    // highest ballot this replica has promised not to go bellow
    private Ballot promisedBallot;

    // ballot this replica is using if trying to become leader
    private Ballot leaderBallot;
    private boolean amLeader;

    // leader host known by this replica
    private Host currentLeader;

    private int ballotNumber;

    private final Map<Integer, PaxosInstance> instances;

    // stores replicas that replied to prepare ex: {self, replica2}
    private final Set<Host> prepareOkQuorum;
    // stores the accepted value with the highest accepted ballot for each instance
    private final Map<Integer, PrepareOkMessage.AcceptedValue> recoveredAcceptedValues;

    private long heartbeatInterval;
    private long electionTimeoutMin;
    private long electionTimeoutMax;

    private long heartbeatTimerId;
    private long leaderTimeoutTimerId;

    private final Random random;

    public MultiPaxos(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.membership = new LinkedList<>();
        this.promisedBallot = null;
        this.leaderBallot = null;
        this.amLeader = false;
        this.currentLeader = null;
        this.ballotNumber = 0;

        this.instances = new HashMap<>();
        this.prepareOkQuorum = new HashSet<>();
        this.recoveredAcceptedValues = new HashMap<>();

        this.heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
        this.electionTimeoutMin = DEFAULT_ELECTION_TIMEOUT_MIN;
        this.electionTimeoutMax = DEFAULT_ELECTION_TIMEOUT_MAX;

        this.heartbeatTimerId = -1;
        this.leaderTimeoutTimerId = -1;

        this.random = new Random();

        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        registerTimerHandler(HeartbeatTimer.TIMER_ID, this::uponHeartbeatTimer);
        registerTimerHandler(LeaderTimeoutTimer.TIMER_ID, this::uponLeaderTimeoutTimer);

        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    @Override
    public void init(Properties props) {
        heartbeatInterval = Long.parseLong(
                props.getProperty("multipaxos.heartbeat_interval", String.valueOf(DEFAULT_HEARTBEAT_INTERVAL)));

        electionTimeoutMin = Long.parseLong(
                props.getProperty("multipaxos.election_timeout_min", String.valueOf(DEFAULT_ELECTION_TIMEOUT_MIN)));

        electionTimeoutMax = Long.parseLong(
                props.getProperty("multipaxos.election_timeout_max", String.valueOf(DEFAULT_ELECTION_TIMEOUT_MAX)));

        if (electionTimeoutMax <= electionTimeoutMin) {
            throw new IllegalArgumentException("multipaxos.election_timeout_max must be greater than min");
        }

    }

    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        this.channelId = notification.getChannelId();
        this.myself = notification.getMyself();

        logger.info("MultiPaxos registered on shared channel {}, I am {}", channelId, myself);

        registerSharedChannel(channelId);

        registerMessageSerializer(channelId, PrepareMessage.MSG_ID, PrepareMessage.serializer);
        registerMessageSerializer(channelId, PrepareOkMessage.MSG_ID, PrepareOkMessage.serializer);
        registerMessageSerializer(channelId, AcceptMessage.MSG_ID, AcceptMessage.serializer);
        registerMessageSerializer(channelId, AcceptOkMessage.MSG_ID, AcceptOkMessage.serializer);
        registerMessageSerializer(channelId, DecisionMessage.MSG_ID, DecisionMessage.serializer);
        registerMessageSerializer(channelId, HeartbeatMessage.MSG_ID, HeartbeatMessage.serializer);

        try {
            registerMessageHandler(channelId, PrepareMessage.MSG_ID,
                    this::uponPrepareMessage, this::uponMsgFail);
            registerMessageHandler(channelId, PrepareOkMessage.MSG_ID,
                    this::uponPrepareOkMessage, this::uponMsgFail);
            registerMessageHandler(channelId, AcceptMessage.MSG_ID,
                    this::uponAcceptMessage, this::uponMsgFail);
            registerMessageHandler(channelId, AcceptOkMessage.MSG_ID,
                    this::uponAcceptOkMessage, this::uponMsgFail);
            registerMessageHandler(channelId, DecisionMessage.MSG_ID,
                    this::uponDecisionMessage, this::uponMsgFail);
            registerMessageHandler(channelId, HeartbeatMessage.MSG_ID,
                    this::uponHeartbeatMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering MultiPaxos handlers.", e);
        }
    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        this.membership = new LinkedList<>(notification.getMembership());

        logger.info("MultiPaxos joined. Membership: {}", membership);

        /*
         * Bootstrap case:
         *
         * There is no deterministic first leader.
         * Since the system is starting from an empty state, there are no previous
         * accepted values to recover, but instead of choosing membership.get(0),
         * every replica starts a randomized leader timeout.
         *
         * The first replica whose timeout expires starts Phase 1.
         * If it receives PrepareOk from a majority, it becomes leader.
         *
         * Later leader changes also use Phase 1.
         */
        amLeader = false;
        currentLeader = null;
        leaderBallot = null;

        resetLeaderTimeout();
    }

    private void startPhase1() {
        amLeader = false;
        currentLeader = null;

        if (promisedBallot != null) {
            ballotNumber = Math.max(ballotNumber, promisedBallot.getNumber());
        }

        ballotNumber++;

        Ballot newBallot = new Ballot(ballotNumber, myself.toString());

        leaderBallot = newBallot;
        promisedBallot = newBallot;

        prepareOkQuorum.clear();
        prepareOkQuorum.add(myself);

        recoveredAcceptedValues.clear();

        logger.info("Starting MultiPaxos Phase 1 with ballot {}", newBallot);

        PrepareMessage prepare = new PrepareMessage(newBallot);

        for (Host h : membership) {
            sendToOther(prepare, h);
        }

        checkPrepareQuorum();
    }

    private void uponPrepareMessage(PrepareMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);

        if (promisedBallot == null || msg.getBallot().greaterOrEqualThan(promisedBallot)) {

            if (amLeader && leaderBallot != null && msg.getBallot().greaterThan(leaderBallot)) {
                stepDown(null, msg.getBallot());
            }

            promisedBallot = msg.getBallot();

            Map<Integer, PrepareOkMessage.AcceptedValue> accepted = new HashMap<>();

            // if follower already accepted something before, new leader must know it
            for (PaxosInstance instance : instances.values()) {
                if (instance.getAcceptedBallot() != null) {
                    accepted.put(instance.getInstance(), new PrepareOkMessage.AcceptedValue(
                            instance.getInstance(),
                            instance.getAcceptedBallot(),
                            instance.getAcceptedOpId(),
                            instance.getAcceptedOperation()));
                }
            }

            openConnection(from);
            sendMessage(new PrepareOkMessage(msg.getBallot(), accepted), from);
        }
    }

    private void uponPrepareOkMessage(PrepareOkMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);

        if (leaderBallot == null || !msg.getBallot().equals(leaderBallot)) {
            return;
        }

        prepareOkQuorum.add(from);

        for (PrepareOkMessage.AcceptedValue value : msg.getAcceptedValues().values()) {
            PrepareOkMessage.AcceptedValue current = recoveredAcceptedValues.get(value.getInstance());

            // If any value may already have been chosen, the new leader must continue with
            // that value
            if (current == null ||
                    value.getAcceptedBallot().greaterThan(current.getAcceptedBallot())) {
                recoveredAcceptedValues.put(value.getInstance(), value);
            }
        }

        checkPrepareQuorum();
    }

    private void checkPrepareQuorum() {
        if (amLeader) {
            return;
        }

        if (prepareOkQuorum.size() >= majority()) {
            amLeader = true;
            currentLeader = myself;

            logger.info("Became MultiPaxos leader with ballot {}", leaderBallot);

            triggerNotification(new LeaderChangeNotification(myself));

            if (leaderTimeoutTimerId != -1) {
                cancelTimer(leaderTimeoutTimerId);
                leaderTimeoutTimerId = -1;
            }

            startHeartbeatTimer();

            /*
             * Recover previously accepted values.
             */
            for (PrepareOkMessage.AcceptedValue value : recoveredAcceptedValues.values()) {
                sendAccept(
                        value.getInstance(),
                        value.getOpId(),
                        value.getOperation());
            }
        }
    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        logger.debug("MultiPaxos received proposal {}", request);

        if (!amLeader) {
            logger.warn("Ignoring proposal because I am not leader. Current leader is {}", currentLeader);
            return;
        }

        sendAccept(request.getInstance(), request.getOpId(), request.getOperation());
    }

    private void sendAccept(int instanceNumber, UUID opId, byte[] operation) {
        PaxosInstance instance = getInstance(instanceNumber);
        instance.clearAcceptOkQuorum();
        instance.getAcceptOkQuorum().add(myself);
        instance.accept(leaderBallot, opId, operation);

        AcceptMessage accept = new AcceptMessage(instanceNumber, leaderBallot, opId, operation);

        for (Host h : membership) {
            sendToOther(accept, h);
        }

        checkAcceptQuorum(instance);
    }

    private void uponAcceptMessage(AcceptMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);

        if (promisedBallot == null || msg.getBallot().greaterOrEqualThan(promisedBallot)) {

            if (amLeader && leaderBallot != null && msg.getBallot().greaterThan(leaderBallot)) {
                stepDown(from, msg.getBallot());
            }

            promisedBallot = msg.getBallot();

            if (!from.equals(currentLeader)) {
                currentLeader = from;
                triggerNotification(new LeaderChangeNotification(from));
            }

            resetLeaderTimeout();

            PaxosInstance instance = getInstance(msg.getInstance());
            instance.accept(msg.getBallot(), msg.getOpId(), msg.getOperation());

            openConnection(from);
            sendMessage(new AcceptOkMessage(msg.getInstance(), msg.getBallot()), from);
        }
    }

    private void uponAcceptOkMessage(AcceptOkMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);

        if (!amLeader || leaderBallot == null || !msg.getBallot().equals(leaderBallot)) {
            return;
        }

        PaxosInstance instance = getInstance(msg.getInstance());
        instance.getAcceptOkQuorum().add(from);

        checkAcceptQuorum(instance);
    }

    private void checkAcceptQuorum(PaxosInstance instance) {
        if (instance.isDecided()) {
            return;
        }

        if (instance.getAcceptOkQuorum().size() >= majority()) {
            instance.decide(instance.getAcceptedOpId(), instance.getAcceptedOperation());

            DecisionMessage decision = new DecisionMessage(
                    instance.getInstance(),
                    instance.getAcceptedOpId(),
                    instance.getAcceptedOperation());

            for (Host h : membership) {
                sendToOther(decision, h);
            }

            deliverDecision(instance);
        }
    }

    private void uponDecisionMessage(DecisionMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);

        PaxosInstance instance = getInstance(msg.getInstance());

        if (!instance.isDecided()) {
            instance.decide(msg.getOpId(), msg.getOperation());
            deliverDecision(instance);
        }
    }

    private void deliverDecision(PaxosInstance instance) {
        triggerNotification(new DecidedNotification(
                instance.getInstance(),
                instance.getDecidedOpId(),
                instance.getDecidedOperation()));
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

    private void resetLeaderTimeout() {
        if (amLeader) {
            return;
        }

        if (leaderTimeoutTimerId != -1) {
            cancelTimer(leaderTimeoutTimerId);
        }

        leaderTimeoutTimerId = setupTimer(
                new LeaderTimeoutTimer(),
                randomElectionTimeout());
    }

    private long randomElectionTimeout() {
        long diff = electionTimeoutMax - electionTimeoutMin;
        return electionTimeoutMin + random.nextInt((int) diff);
    }

    private void uponHeartbeatTimer(HeartbeatTimer timer, long timerId) {
        if (!amLeader) {
            return;
        }

        HeartbeatMessage heartbeat = new HeartbeatMessage(leaderBallot);

        for (Host h : membership) {
            sendToOther(heartbeat, h);
        }
    }

    private void uponLeaderTimeoutTimer(LeaderTimeoutTimer timer, long timerId) {
        if (timerId != leaderTimeoutTimerId) {
            return;
        }

        if (amLeader) {
            return;
        }

        logger.warn("Leader timeout expired. Starting Phase 1. Previous leader was {}", currentLeader);

        startPhase1();

        /*
         * If this election does not reach a majority, retry later with a higher ballot.
         * If it succeeds, checkPrepareQuorum() cancels this timer and starts
         * heartbeats.
         */
        resetLeaderTimeout();
    }

    private void uponHeartbeatMessage(HeartbeatMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);

        Ballot heartbeatBallot = msg.getBallot();

        if (promisedBallot != null && promisedBallot.greaterThan(heartbeatBallot)) {
            logger.debug("Ignoring heartbeat from lower ballot {}", heartbeatBallot);
            return;
        }

        if (amLeader && leaderBallot != null && heartbeatBallot.greaterThan(leaderBallot)) {
            stepDown(from, heartbeatBallot);
        }

        promisedBallot = heartbeatBallot;

        if (!from.equals(currentLeader)) {
            currentLeader = from;
            triggerNotification(new LeaderChangeNotification(from));
        }

        resetLeaderTimeout();
    }

    private void stepDown(Host newLeader, Ballot newBallot) {
        logger.info("Stepping down. New leader {}, new ballot {}", newLeader, newBallot);

        amLeader = false;
        currentLeader = newLeader;
        leaderBallot = null;
        promisedBallot = newBallot;

        if (heartbeatTimerId != -1) {
            cancelTimer(heartbeatTimerId);
            heartbeatTimerId = -1;
        }

        if (newLeader != null) {
            triggerNotification(new LeaderChangeNotification(newLeader));
        }

        resetLeaderTimeout();
    }

    private void sendToOther(ProtoMessage msg, Host h) {
        if (h.equals(myself)) {
            return;
        }

        openConnection(h);
        sendMessage(msg, h);
    }

    private PaxosInstance getInstance(int instanceNumber) {
        return instances.computeIfAbsent(instanceNumber, PaxosInstance::new);
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