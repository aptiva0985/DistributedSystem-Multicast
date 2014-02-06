package distSysLab2.timeStamp;

import java.util.concurrent.atomic.AtomicInteger;

public class LogicalTimeStamp extends TimeStamp implements Comparable<LogicalTimeStamp> {
    private static final long serialVersionUID = 1L;

    private AtomicInteger localTS;

    public LogicalTimeStamp() {
        localTS = new AtomicInteger(0);
    }

    @Override
    public int compareTo(LogicalTimeStamp ts) {
        // For logical time stamp, we only compare the value.
        return this.localTS.get() - ((AtomicInteger)ts.getTimeStamp()).get();
    }

    @Override
    public AtomicInteger getTimeStamp() {
        return this.localTS;
    }

    @Override
    public void setTimeStamp(Object ts) {
        this.localTS = (AtomicInteger)ts;
    }

    @Override
    public String toString() {
        return this.localTS.toString();
    }
}
