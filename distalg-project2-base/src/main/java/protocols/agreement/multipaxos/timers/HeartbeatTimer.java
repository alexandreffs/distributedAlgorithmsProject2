package protocols.agreement.multipaxos.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class HeartbeatTimer extends ProtoTimer {

    public static final short TIMER_ID = 401;

    public HeartbeatTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return new HeartbeatTimer();
    }
}