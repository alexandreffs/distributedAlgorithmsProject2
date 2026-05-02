package protocols.agreement.multipaxos.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.multipaxos.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class AcceptMessage extends ProtoMessage {

    public static final short MSG_ID = 403;

    private final int instance;
    private final Ballot ballot;
    private final UUID opId;
    private final byte[] operation;

    public AcceptMessage(int instance, Ballot ballot, UUID opId, byte[] operation) {
        super(MSG_ID);
        this.instance = instance;
        this.ballot = ballot;
        this.opId = opId;
        this.operation = operation;
    }

    public int getInstance() {
        return instance;
    }

    public Ballot getBallot() {
        return ballot;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOperation() {
        return operation;
    }

    public static final ISerializer<AcceptMessage> serializer = new ISerializer<AcceptMessage>() {
        @Override
        public void serialize(AcceptMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);

            out.writeInt(msg.ballot.getNumber());
            byte[] leaderBytes = msg.ballot.getLeaderId().getBytes();
            out.writeInt(leaderBytes.length);
            out.writeBytes(leaderBytes);

            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());

            out.writeInt(msg.operation.length);
            out.writeBytes(msg.operation);
        }

        @Override
        public AcceptMessage deserialize(ByteBuf in) {
            int instance = in.readInt();

            int ballotNumber = in.readInt();
            byte[] leaderBytes = new byte[in.readInt()];
            in.readBytes(leaderBytes);
            String leaderId = new String(leaderBytes);

            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);

            byte[] operation = new byte[in.readInt()];
            in.readBytes(operation);

            return new AcceptMessage(
                    instance,
                    new Ballot(ballotNumber, leaderId),
                    opId,
                    operation);
        }
    };

    @Override
    public String toString() {
        return "AcceptMessage{" +
                "instance=" + instance +
                ", ballot=" + ballot +
                ", opId=" + opId +
                '}';
    }
}