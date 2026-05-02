package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.UUID;

public class ForwardRequestMessage extends ProtoMessage {

    public static final short MSG_ID = 201;

    private final UUID opId;
    private final byte[] operation;

    public ForwardRequestMessage(UUID opId, byte[] operation) {
        super(MSG_ID);
        this.opId = opId;
        this.operation = operation;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOperation() {
        return operation;
    }

    public static final ISerializer<ForwardRequestMessage> serializer = new ISerializer<ForwardRequestMessage>() {
        @Override
        public void serialize(ForwardRequestMessage msg, ByteBuf out) throws IOException {
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());

            out.writeInt(msg.operation.length);
            out.writeBytes(msg.operation);
        }

        @Override
        public ForwardRequestMessage deserialize(ByteBuf in) throws IOException {
            long most = in.readLong();
            long least = in.readLong();
            UUID opId = new UUID(most, least);

            byte[] operation = new byte[in.readInt()];
            in.readBytes(operation);

            return new ForwardRequestMessage(opId, operation);
        }
    };

    @Override
    public String toString() {
        return "ForwardRequestMessage{" +
                "opId=" + opId +
                ", operation=" + Hex.encodeHexString(operation) +
                '}';
    }
}