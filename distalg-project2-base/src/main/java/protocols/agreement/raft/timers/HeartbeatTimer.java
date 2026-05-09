package protocols.agreement.raft.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class HeartbeatTimer extends ProtoTimer {

    public static final short TIMER_ID = 502;

    public HeartbeatTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return new HeartbeatTimer();
    }
}