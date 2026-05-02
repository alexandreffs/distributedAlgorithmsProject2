package protocols.agreement.multipaxos.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.multipaxos.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PrepareMessage extends ProtoMessage {

    public static final short MSG_ID = 401;

    private final Ballot ballot;

    public PrepareMessage(Ballot ballot) {
        super(MSG_ID);
        this.ballot = ballot;
    }

    public Ballot getBallot() {
        return ballot;
    }

    public static final ISerializer<PrepareMessage> serializer = new ISerializer<PrepareMessage>() {
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) {
            out.writeInt(msg.ballot.getNumber());

            byte[] leaderBytes = msg.ballot.getLeaderId().getBytes();
            out.writeInt(leaderBytes.length);
            out.writeBytes(leaderBytes);
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
            int number = in.readInt();

            byte[] leaderBytes = new byte[in.readInt()];
            in.readBytes(leaderBytes);
            String leaderId = new String(leaderBytes);

            return new PrepareMessage(new Ballot(number, leaderId));
        }
    };

    @Override
    public String toString() {
        return "PrepareMessage{" +
                "ballot=" + ballot +
                '}';
    }
}