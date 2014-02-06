package distSysLab2.clock;

import java.util.concurrent.atomic.AtomicInteger;

import distSysLab2.timeStamp.LogicalTimeStamp;
import distSysLab2.timeStamp.TimeStamp;

public class LogicalClockService extends ClockService {
    public LogicalClockService(int nodeAmount) {
        this.curTimeStamp = new LogicalTimeStamp();
    }

    @Override
    public void updateTimeStampOnSend() {
        AtomicInteger cur = (AtomicInteger)this.getCurTimeStamp().getTimeStamp();
        cur.addAndGet(step);
        this.getCurTimeStamp().setTimeStamp(cur);
    }

    @Override
    public void updateTimeStampOnReceive(TimeStamp ts) {
        AtomicInteger localTS = (AtomicInteger)this.getCurTimeStamp().getTimeStamp();
        AtomicInteger remoteTS = (AtomicInteger)ts.getTimeStamp();

        int localVal = localTS.get();
        int remoteVal = remoteTS.get();

        // Update is based on a "max plus one" manner
        AtomicInteger newVal = new AtomicInteger(Math.max(localVal, remoteVal) + 1);
        this.curTimeStamp.setTimeStamp(newVal);
    }
}
