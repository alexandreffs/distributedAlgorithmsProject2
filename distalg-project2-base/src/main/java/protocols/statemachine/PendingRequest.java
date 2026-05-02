package protocols.statemachine;

import java.util.Arrays;
import java.util.UUID;

public class PendingRequest {

    private final UUID opId;
    private final byte[] operation;

    public PendingRequest(UUID opId, byte[] operation) {
        this.opId = opId;
        this.operation = operation;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOperation() {
        return operation;
    }

    @Override
    public String toString() {
        return "PendingRequest{" +
                "opId=" + opId +
                ", operation=" + Arrays.toString(operation) +
                '}';
    }
}