package protocols.agreement.multipaxos.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.multipaxos.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class PrepareOkMessage extends ProtoMessage {

    public static final short MSG_ID = 402;

    public static class AcceptedValue {
        private final int instance;
        private final Ballot acceptedBallot;
        private final UUID opId;
        private final byte[] operation;

        public AcceptedValue(int instance, Ballot acceptedBallot, UUID opId, byte[] operation) {
            this.instance = instance;
            this.acceptedBallot = acceptedBallot;
            this.opId = opId;
            this.operation = operation;
        }

        public int getInstance() {
            return instance;
        }

        public Ballot getAcceptedBallot() {
            return acceptedBallot;
        }

        public UUID getOpId() {
            return opId;
        }

        public byte[] getOperation() {
            return operation;
        }

        @Override
        public String toString() {
            return "AcceptedValue{" +
                    "instance=" + instance +
                    ", acceptedBallot=" + acceptedBallot +
                    ", opId=" + opId +
                    '}';
        }
    }

    private final Ballot ballot;
    private final Map<Integer, AcceptedValue> acceptedValues;

    public PrepareOkMessage(Ballot ballot, Map<Integer, AcceptedValue> acceptedValues) {
        super(MSG_ID);
        this.ballot = ballot;
        this.acceptedValues = acceptedValues;
    }

    public Ballot getBallot() {
        return ballot;
    }

    public Map<Integer, AcceptedValue> getAcceptedValues() {
        return acceptedValues;
    }

    private static void writeString(ByteBuf out, String value) {
        byte[] bytes = value.getBytes();
        out.writeInt(bytes.length);
        out.writeBytes(bytes);
    }

    private static String readString(ByteBuf in) {
        byte[] bytes = new byte[in.readInt()];
        in.readBytes(bytes);
        return new String(bytes);
    }

    private static void writeBallot(ByteBuf out, Ballot ballot) {
        out.writeInt(ballot.getNumber());
        writeString(out, ballot.getLeaderId());
    }

    private static Ballot readBallot(ByteBuf in) {
        int number = in.readInt();
        String leaderId = readString(in);
        return new Ballot(number, leaderId);
    }

    public static final ISerializer<PrepareOkMessage> serializer = new ISerializer<PrepareOkMessage>() {
        @Override
        public void serialize(PrepareOkMessage msg, ByteBuf out) {
            writeBallot(out, msg.ballot);

            out.writeInt(msg.acceptedValues.size());

            for (AcceptedValue value : msg.acceptedValues.values()) {
                out.writeInt(value.instance);
                writeBallot(out, value.acceptedBallot);

                out.writeLong(value.opId.getMostSignificantBits());
                out.writeLong(value.opId.getLeastSignificantBits());

                out.writeInt(value.operation.length);
                out.writeBytes(value.operation);
            }
        }

        @Override
        public PrepareOkMessage deserialize(ByteBuf in) {
            Ballot ballot = readBallot(in);

            int size = in.readInt();
            Map<Integer, AcceptedValue> acceptedValues = new HashMap<>();

            for (int i = 0; i < size; i++) {
                int instance = in.readInt();
                Ballot acceptedBallot = readBallot(in);

                long highBytes = in.readLong();
                long lowBytes = in.readLong();
                UUID opId = new UUID(highBytes, lowBytes);

                byte[] operation = new byte[in.readInt()];
                in.readBytes(operation);

                acceptedValues.put(instance,
                        new AcceptedValue(instance, acceptedBallot, opId, operation));
            }

            return new PrepareOkMessage(ballot, acceptedValues);
        }
    };

    @Override
    public String toString() {
        return "PrepareOkMessage{" +
                "ballot=" + ballot +
                ", acceptedValues=" + acceptedValues +
                '}';
    }
}