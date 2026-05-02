package protocols.agreement.multipaxos.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.multipaxos.Ballot;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class AcceptOkMessage extends ProtoMessage {

    public static final short MSG_ID = 404;

    private final int instance;
    private final Ballot ballot;

    public AcceptOkMessage(int instance, Ballot ballot) {
        super(MSG_ID);
        this.instance = instance;
        this.ballot = ballot;
    }

    public int getInstance() {
        return instance;
    }

    public Ballot getBallot() {
        return ballot;
    }

    public static final ISerializer<AcceptOkMessage> serializer = new ISerializer<AcceptOkMessage>() {
        @Override
        public void serialize(AcceptOkMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);

            out.writeInt(msg.ballot.getNumber());
            byte[] leaderBytes = msg.ballot.getLeaderId().getBytes();
            out.writeInt(leaderBytes.length);
            out.writeBytes(leaderBytes);
        }

        @Override
        public AcceptOkMessage deserialize(ByteBuf in) {
            int instance = in.readInt();

            int ballotNumber = in.readInt();
            byte[] leaderBytes = new byte[in.readInt()];
            in.readBytes(leaderBytes);
            String leaderId = new String(leaderBytes);

            return new AcceptOkMessage(instance, new Ballot(ballotNumber, leaderId));
        }
    };

    @Override
    public String toString() {
        return "AcceptOkMessage{" +
                "instance=" + instance +
                ", ballot=" + ballot +
                '}';
    }
}