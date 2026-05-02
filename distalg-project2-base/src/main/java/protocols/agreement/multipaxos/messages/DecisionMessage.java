package protocols.agreement.multipaxos.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class DecisionMessage extends ProtoMessage {

    public static final short MSG_ID = 405;

    private final int instance;
    private final UUID opId;
    private final byte[] operation;

    public DecisionMessage(int instance, UUID opId, byte[] operation) {
        super(MSG_ID);
        this.instance = instance;
        this.opId = opId;
        this.operation = operation;
    }

    public int getInstance() {
        return instance;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOperation() {
        return operation;
    }

    public static final ISerializer<DecisionMessage> serializer = new ISerializer<DecisionMessage>() {
        @Override
        public void serialize(DecisionMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);

            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());

            out.writeInt(msg.operation.length);
            out.writeBytes(msg.operation);
        }

        @Override
        public DecisionMessage deserialize(ByteBuf in) {
            int instance = in.readInt();

            long most = in.readLong();
            long least = in.readLong();
            UUID opId = new UUID(most, least);

            byte[] operation = new byte[in.readInt()];
            in.readBytes(operation);

            return new DecisionMessage(instance, opId, operation);
        }
    };

    @Override
    public String toString() {
        return "DecisionMessage{" +
                "instance=" + instance +
                ", opId=" + opId +
                '}';
    }
}