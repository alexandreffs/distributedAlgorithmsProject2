package protocols.agreement.multipaxos;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class PaxosInstance {

    private final int instance;

    private Ballot promisedBallot;
    private Ballot acceptedBallot;

    private UUID acceptedOpId;
    private byte[] acceptedOperation;

    private UUID decidedOpId;
    private byte[] decidedOperation;

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