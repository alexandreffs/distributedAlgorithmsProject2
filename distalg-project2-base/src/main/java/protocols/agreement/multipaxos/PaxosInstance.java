package protocols.agreement.multipaxos;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class PaxosInstance {

    // log slot number
    private final int instance;

    // highest ballot this replica promised for this instance
    private Ballot promisedBallot;

    // store the value this replica accepted for this instance
    private Ballot acceptedBallot;
    private UUID acceptedOpId;
    private byte[] acceptedOperation;

    // store final decision for this slot
    private UUID decidedOpId;
    private byte[] decidedOperation;

    // used by leader to see the replies to the AcceptMessage
    // it stores who replied ex: acceptOkQuorum = {replica1, replica2}
    private final Set<Host> acceptOkQuorum;

    public PaxosInstance(int instance) {
        this.instance = instance;
        this.promisedBallot = null;
        this.acceptedBallot = null;
        this.acceptedOpId = null;
        this.acceptedOperation = null;
        this.decidedOpId = null;
        this.decidedOperation = null;
        this.acceptOkQuorum = new HashSet<>();
    }

    public int getInstance() {
        return instance;
    }

    public Ballot getPromisedBallot() {
        return promisedBallot;
    }

    public void setPromisedBallot(Ballot promisedBallot) {
        this.promisedBallot = promisedBallot;
    }

    public Ballot getAcceptedBallot() {
        return acceptedBallot;
    }

    public UUID getAcceptedOpId() {
        return acceptedOpId;
    }

    public byte[] getAcceptedOperation() {
        return acceptedOperation;
    }

    // records that replica accepted a value
    public void accept(Ballot ballot, UUID opId, byte[] operation) {
        this.promisedBallot = ballot;
        this.acceptedBallot = ballot;
        this.acceptedOpId = opId;
        this.acceptedOperation = operation;
    }

    public boolean isDecided() {
        return decidedOperation != null;
    }

    public UUID getDecidedOpId() {
        return decidedOpId;
    }

    public byte[] getDecidedOperation() {
        return decidedOperation;
    }

    // records final decision
    public void decide(UUID opId, byte[] operation) {
        this.decidedOpId = opId;
        this.decidedOperation = operation;
    }

    public Set<Host> getAcceptOkQuorum() {
        return acceptOkQuorum;
    }

    public void clearAcceptOkQuorum() {
        acceptOkQuorum.clear();
    }
}