package protocols.agreement.raft;

import org.apache.commons.codec.binary.Hex;

import java.util.UUID;

public class LogEntry {

    private final int index;
    private final int term;
    private final UUID opId;
    private final byte[] operation;

    public LogEntry(int index, int term, UUID opId, byte[] operation) {
        this.index = index;
        this.term = term;
        this.opId = opId;
        this.operation = operation;
    }

    public int getIndex() {
        return index;
    }

    public int getTerm() {
        return term;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOperation() {
        return operation;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "index=" + index +
                ", term=" + term +
                ", opId=" + opId +
                ", operation=" + Hex.encodeHexString(operation) +
                '}';
    }
}