package protocols.agreement.raft.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class RequestVoteMessage extends ProtoMessage {

    public static final short MSG_ID = 301;

    private final int term;
    private final int lastLogIndex;
    private final int lastLogTerm;

    public RequestVoteMessage(int term, int lastLogIndex, int lastLogTerm) {
        super(MSG_ID);
        this.term = term;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getTerm() {
        return term;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    public static final ISerializer<RequestVoteMessage> serializer = new ISerializer<RequestVoteMessage>() {
        @Override
        public void serialize(RequestVoteMessage msg, ByteBuf out) {
            out.writeInt(msg.term);
            out.writeInt(msg.lastLogIndex);
            out.writeInt(msg.lastLogTerm);
        }

        @Override
        public RequestVoteMessage deserialize(ByteBuf in) {
            int term = in.readInt();
            int lastLogIndex = in.readInt();
            int lastLogTerm = in.readInt();

            return new RequestVoteMessage(term, lastLogIndex, lastLogTerm);
        }
    };

    @Override
    public String toString() {
        return "RequestVoteMessage{" +
                "term=" + term +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}