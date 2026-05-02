package protocols.statemachine;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.agreement.AgreementProtocol;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.notifications.LeaderChangeNotification;
import protocols.agreement.requests.ProposeRequest;
import protocols.statemachine.messages.ForwardRequestMessage;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.statemachine.notifications.ClientRequestReply;
import protocols.statemachine.requests.OrderRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

public class StateMachine extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(StateMachine.class);

    private enum State {
        JOINING,
        ACTIVE
    }

    public static final String PROTOCOL_NAME = "StateMachine";
    public static final short PROTOCOL_ID = 200;

    private final Host self;
    private final int channelId;

    private State state;
    private List<Host> membership;

    /*
     * Instance management.
     *
     * nextInstanceToPropose:
     * Only meaningful when this replica is the leader. It is the next agreement
     * instance where this SMR will propose an operation.
     *
     * nextInstanceToExecute:
     * The next instance that must be executed by the application. Decisions may
     * arrive out of order, but the state machine must execute them strictly in
     * increasing instance order.
     */
    private int nextInstanceToPropose;
    private int nextInstanceToExecute;

    /*
     * Current leader according to the agreement protocol.
     */
    private Host currentLeader;

    /*
     * Local client requests that have not yet been executed.
     *
     * These are requests originally received from the local application layer.
     * If this replica is not the leader, the request is forwarded, but we still
     * keep it here so that when the operation is eventually decided we can notify
     * the local application.
     */
    private final Map<UUID, PendingRequest> pendingLocalRequests;

    /*
     * Decisions that arrived but cannot yet be executed because previous
     * instances have not been executed.
     */
    private final Map<Integer, DecidedNotification> decidedBuffer;

    /*
     * Operations already executed.
     *
     * This avoids sending duplicate replies for the same client operation if the
     * operation is resubmitted after a leader change and ends up appearing more
     * than once in the log.
     */
    private final Set<UUID> executedOperations;

    public StateMachine(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.nextInstanceToPropose = 0;
        this.nextInstanceToExecute = 0;
        this.currentLeader = null;

        this.membership = new LinkedList<>();
        this.pendingLocalRequests = new LinkedHashMap<>();
        this.decidedBuffer = new HashMap<>();
        this.executedOperations = new HashSet<>();

        String address = props.getProperty("babel.address");
        String port = props.getProperty("babel.port");

        logger.info("Listening on {}:{}", address, port);
        this.self = new Host(InetAddress.getByName(address), Integer.parseInt(port));

        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
        channelProps.setProperty(TCPChannel.PORT_KEY, port);
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");

        this.channelId = createChannel(TCPChannel.NAME, channelProps);

        /* -------------------- Register Channel Events -------------------- */

        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /* -------------------- Register StateMachine messages -------------------- */

        registerMessageSerializer(channelId, ForwardRequestMessage.MSG_ID, ForwardRequestMessage.serializer);

        registerMessageHandler(
                channelId,
                ForwardRequestMessage.MSG_ID,
                this::uponForwardRequestMessage,
                this::uponMsgFail);

        /* -------------------- Register Request Handlers -------------------- */

        registerRequestHandler(OrderRequest.REQUEST_ID, this::uponOrderRequest);

        /* -------------------- Register Notification Handlers -------------------- */

        subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);
        subscribeNotification(LeaderChangeNotification.NOTIFICATION_ID, this::uponLeaderChangeNotification);
    }

    @Override
    public void init(Properties props) {
        /*
         * Inform the agreement protocol and the application that the TCP channel is
         * ready.
         */
        triggerNotification(new ChannelReadyNotification(channelId, self));

        String host = props.getProperty("initial_membership");

        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Missing property initial_membership");
        }

        String[] hosts = host.split(",");
        List<Host> initialMembership = new LinkedList<>();

        for (String s : hosts) {
            String[] hostElements = s.trim().split(":");

            if (hostElements.length != 2) {
                throw new IllegalArgumentException("Invalid host in initial_membership: " + s);
            }

            Host h;

            try {
                h = new Host(
                        InetAddress.getByName(hostElements[0]),
                        Integer.parseInt(hostElements[1]));
            } catch (UnknownHostException e) {
                throw new AssertionError("Error parsing initial_membership", e);
            }

            initialMembership.add(h);
        }

        if (initialMembership.contains(self)) {
            state = State.ACTIVE;
            membership = new LinkedList<>(initialMembership);

            logger.info("Starting in ACTIVE. Membership: {}", membership);

            for (Host h : membership) {
                if (!h.equals(self)) {
                    openConnection(h);
                }
            }

            /*
             * Notify the agreement protocol that we are part of the initial
             * membership.
             */
            triggerNotification(new JoinedNotification(membership, 0));
        } else {
            state = State.JOINING;
            logger.info("Starting in JOINING because I am not part of initial membership");

            /*
             * Dynamic membership/state transfer is bonus. For the base project, all
             * replicas should normally be in the initial membership.
             */
        }
    }

    /*
     * -------------------------------------------------------------------------
     * Requests from the application
     * ----------------------------------------------------------------------
     */

    private void uponOrderRequest(OrderRequest request, short sourceProto) {
        logger.debug("Received OrderRequest: {}", request);

        if (state == State.JOINING) {
            logger.warn("Ignoring client request while JOINING: {}", request.getOpId());
            return;
        }

        /*
         * Store local request as pending. Even if this replica is not leader, the
         * local application expects a reply once this operation is executed.
         */
        pendingLocalRequests.putIfAbsent(
                request.getOpId(),
                new PendingRequest(request.getOpId(), request.getOperation()));

        submitOrForward(request.getOpId(), request.getOperation());
    }

    /*
     * -------------------------------------------------------------------------
     * Messages between StateMachine replicas
     * ----------------------------------------------------------------------
     */

    private void uponForwardRequestMessage(
            ForwardRequestMessage msg,
            Host from,
            short sourceProto,
            int channelId) {
        logger.debug("Received ForwardRequestMessage from {}: {}", from, msg);

        if (state != State.ACTIVE) {
            logger.warn("Ignoring forwarded request while not ACTIVE: {}", msg.getOpId());
            return;
        }

        if (isLeader()) {
            proposeToAgreement(msg.getOpId(), msg.getOperation());
        } else {
            /*
             * This can happen if a replica forwarded to an old leader. Forward again
             * to the current leader if we know one.
             */
            logger.debug("Received forwarded request but I am not leader. Current leader: {}", currentLeader);
            forwardToLeader(msg.getOpId(), msg.getOperation());
        }
    }

    /*
     * -------------------------------------------------------------------------
     * Notifications from agreement
     * ----------------------------------------------------------------------
     */

    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        logger.debug("Received DecidedNotification: {}", notification);

        int instance = notification.getInstance();

        if (instance < nextInstanceToExecute) {
            logger.debug("Ignoring old decision for already executed instance {}", instance);
            return;
        }

        DecidedNotification previous = decidedBuffer.putIfAbsent(instance, notification);

        if (previous != null) {
            logger.debug("Already had decision for instance {}. Ignoring duplicate.", instance);
            return;
        }

        executeReadyDecisions();
    }

    private void uponLeaderChangeNotification(LeaderChangeNotification notification, short sourceProto) {
        Host newLeader = notification.getLeaderID();

        logger.info("Leader changed from {} to {}", currentLeader, newLeader);

        this.currentLeader = newLeader;

        if (currentLeader != null && !currentLeader.equals(self) && membership.contains(currentLeader)) {
            openConnection(currentLeader);
        }

        /*
         * Any local request that is still pending may have been sent to an old leader
         * and may never be decided. Resubmit it to the new leader.
         */
        resubmitPendingLocalRequests();
    }

    /*
     * -------------------------------------------------------------------------
     * Core SMR logic
     * ----------------------------------------------------------------------
     */

    private void submitOrForward(UUID opId, byte[] operation) {
        if (currentLeader == null) {
            logger.warn("No known leader yet. Request {} will stay pending.", opId);
            return;
        }

        if (isLeader()) {
            proposeToAgreement(opId, operation);
        } else {
            forwardToLeader(opId, operation);
        }
    }

    private void proposeToAgreement(UUID opId, byte[] operation) {
        int instance = nextInstanceToPropose++;

        logger.debug("Proposing op {} in instance {}", opId, instance);

        sendRequest(
                new ProposeRequest(instance, opId, operation),
                AgreementProtocol.PROTOCOL_ID);
    }

    private void forwardToLeader(UUID opId, byte[] operation) {
        if (currentLeader == null) {
            logger.warn("Cannot forward request {} because leader is unknown", opId);
            return;
        }

        if (currentLeader.equals(self)) {
            proposeToAgreement(opId, operation);
            return;
        }

        logger.debug("Forwarding request {} to leader {}", opId, currentLeader);

        openConnection(currentLeader);
        sendMessage(new ForwardRequestMessage(opId, operation), currentLeader);
    }

    private void resubmitPendingLocalRequests() {
        if (pendingLocalRequests.isEmpty()) {
            return;
        }

        logger.info("Resubmitting {} pending local requests", pendingLocalRequests.size());

        /*
         * Copy the values to avoid concurrent modification if decisions arrive while
         * we are iterating.
         */
        List<PendingRequest> pending = new ArrayList<>(pendingLocalRequests.values());

        for (PendingRequest request : pending) {
            if (!executedOperations.contains(request.getOpId())) {
                submitOrForward(request.getOpId(), request.getOperation());
            }
        }
    }

    private void executeReadyDecisions() {
        while (decidedBuffer.containsKey(nextInstanceToExecute)) {
            DecidedNotification decision = decidedBuffer.remove(nextInstanceToExecute);

            executeDecision(decision);

            nextInstanceToExecute++;
        }
    }

    private void executeDecision(DecidedNotification decision) {
        UUID opId = decision.getOpId();

        /*
         * The operation must be applied to the application state in log order.
         *
         * If the same opId appears again because it was resubmitted after a leader
         * change, we skip the duplicate notification. This avoids replying twice for
         * the same client request.
         */
        if (executedOperations.contains(opId)) {
            logger.debug(
                    "Skipping duplicate operation {} at instance {}",
                    opId,
                    decision.getInstance());
            return;
        }

        executedOperations.add(opId);

        pendingLocalRequests.remove(opId);

        logger.debug(
                "Executing operation {} from instance {}",
                opId,
                decision.getInstance());

        triggerNotification(
                new ClientRequestReply(
                        decision.getOpId(),
                        decision.getOperation()));
    }

    private boolean isLeader() {
        return currentLeader != null && currentLeader.equals(self);
    }

    /*
     * -------------------------------------------------------------------------
     * Message failures
     * ----------------------------------------------------------------------
     */

    private void uponMsgFail(
            ProtoMessage msg,
            Host host,
            short destProto,
            Throwable throwable,
            int channelId) {
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /*
     * -------------------------------------------------------------------------
     * TCP Channel Events
     * ----------------------------------------------------------------------
     */

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("Connection to {} is up", event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.debug("Connection to {} is down, cause {}", event.getNode(), event.getCause());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed, cause: {}", event.getNode(), event.getCause());

        if (membership != null && membership.contains(event.getNode())) {
            openConnection(event.getNode());
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }
}