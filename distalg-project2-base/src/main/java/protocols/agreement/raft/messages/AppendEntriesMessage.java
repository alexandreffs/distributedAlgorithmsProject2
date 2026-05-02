package protocols.agreement.raft.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.raft.LogEntry;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class AppendEntriesMessage extends ProtoMessage {

    public static final short MSG_ID = 303;

    private final int term;
    private final int prevLogIndex;
    private final int prevLogTerm;
    private final int leaderCommit;
    private final List<LogEntry> entries;

    public AppendEntriesMessage(int term, int prevLogIndex, int prevLogTerm,
            int leaderCommit, List<LogEntry> entries) {
        super(MSG_ID);
        this.term = term;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.leaderCommit = leaderCommit;
        this.entries = entries;
    }

    public int getTerm() {
        return term;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }

    public boolean isHeartbeat() {
        return entries.isEmpty();
    }

    public static final ISerializer<AppendEntriesMessage> serializer = new ISerializer<AppendEntriesMessage>() {
        @Override
        public void serialize(AppendEntriesMessage msg, ByteBuf out) {
            out.writeInt(msg.term);
            out.writeInt(msg.prevLogIndex);
            out.writeInt(msg.prevLogTerm);
            out.writeInt(msg.leaderCommit);

            out.writeInt(msg.entries.size());

            for (LogEntry entry : msg.entries) {
                out.writeInt(entry.getIndex());
                out.writeInt(entry.getTerm());

                out.writeLong(entry.getOpId().getMostSignificantBits());
                out.writeLong(entry.getOpId().getLeastSignificantBits());

                out.writeInt(entry.getOperation().length);
                out.writeBytes(entry.getOperation());
            }
        }

        @Override
        public AppendEntriesMessage deserialize(ByteBuf in) {
            int term = in.readInt();
            int prevLogIndex = in.readInt();
            int prevLogTerm = in.readInt();
            int leaderCommit = in.readInt();

            int size = in.readInt();
            List<LogEntry> entries = new ArrayList<>(size);

            for (int i = 0; i < size; i++) {
                int index = in.readInt();
                int entryTerm = in.readInt();

                long most = in.readLong();
                long least = in.readLong();
                UUID opId = new UUID(most, least);

                byte[] operation = new byte[in.readInt()];
                in.readBytes(operation);

                entries.add(new LogEntry(index, entryTerm, opId, operation));
            }

            return new AppendEntriesMessage(term, prevLogIndex, prevLogTerm, leaderCommit, entries);
        }
    };

    @Override
    public String toString() {
        return "AppendEntriesMessage{" +
                "term=" + term +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", leaderCommit=" + leaderCommit +
                ", entries=" + entries +
                '}';
    }
}