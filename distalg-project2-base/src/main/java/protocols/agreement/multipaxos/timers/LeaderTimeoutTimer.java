package protocols.agreement.multipaxos.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class LeaderTimeoutTimer extends ProtoTimer {

    public static final short TIMER_ID = 402;

    public LeaderTimeoutTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return new LeaderTimeoutTimer();
    }
}