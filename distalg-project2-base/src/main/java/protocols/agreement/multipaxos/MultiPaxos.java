package protocols.agreement.multipaxos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.AgreementProtocol;
import protocols.agreement.multipaxos.messages.*;
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

    private Host myself;
    private List<Host> membership;
    private int channelId;

    private Ballot promisedBallot;
    private Ballot leaderBallot;
    private boolean amLeader;
    private Host currentLeader;

    private int ballotNumber;

    private final Map<Integer, PaxosInstance> instances;

    private final Set<Host> prepareOkQuorum;
    private final Map<Integer, PrepareOkMessage.AcceptedValue> recoveredAcceptedValues;

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

        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    @Override
    public void init(Properties props) {
        // TODO: add leader election timeout/retry logic.
        // This first version chooses a deterministic initial leader.
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
         * The first replica in the initial membership is the initial leader.
         * Since the system is starting from an empty state, there are no previous
         * accepted values to recover, so the initial leader can safely start with
         * ballot 1.
         *
         * Later leader changes should still use Phase 1.
         */
        Host first = membership.get(0);

        if (myself.equals(first)) {
            becomeInitialLeader();
        } else {
            amLeader = false;
            currentLeader = first;
            triggerNotification(new LeaderChangeNotification(first));
        }
    }

    private void becomeInitialLeader() {
        ballotNumber = 1;

        Ballot initialBallot = new Ballot(ballotNumber, myself.toString());

        leaderBallot = initialBallot;
        promisedBallot = initialBallot;

        amLeader = true;
        currentLeader = myself;

        logger.info("Became initial MultiPaxos leader with ballot {}", leaderBallot);

        triggerNotification(new LeaderChangeNotification(myself));
    }

    private void startPhase1() {
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
            if (!h.equals(myself)) {
                sendMessage(prepare, h);
            }
        }

        checkPrepareQuorum();
    }

    private void uponPrepareMessage(PrepareMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);

        if (promisedBallot == null || msg.getBallot().greaterThan(promisedBallot)) {
            promisedBallot = msg.getBallot();

            Map<Integer, PrepareOkMessage.AcceptedValue> accepted = new HashMap<>();

            for (PaxosInstance instance : instances.values()) {
                if (instance.getAcceptedBallot() != null && !instance.isDecided()) {
                    accepted.put(instance.getInstance(), new PrepareOkMessage.AcceptedValue(
                            instance.getInstance(),
                            instance.getAcceptedBallot(),
                            instance.getAcceptedOpId(),
                            instance.getAcceptedOperation()));
                }
            }

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
            if (!h.equals(myself)) {
                sendMessage(accept, h);
            }
        }

        checkAcceptQuorum(instance);
    }

    private void uponAcceptMessage(AcceptMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);

        if (promisedBallot == null || msg.getBallot().greaterOrEqualThan(promisedBallot)) {
            promisedBallot = msg.getBallot();

            PaxosInstance instance = getInstance(msg.getInstance());
            instance.accept(msg.getBallot(), msg.getOpId(), msg.getOperation());

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
                if (!h.equals(myself)) {
                    sendMessage(decision, h);
                }
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