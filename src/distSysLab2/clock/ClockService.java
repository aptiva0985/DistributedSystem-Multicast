package distSysLab2.clock;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import distSysLab2.model.NodeBean;
import distSysLab2.timeStamp.TimeStamp;

public abstract class ClockService {
    protected static final int step = 1;

    public enum ClockType {
        LOGICAL, VECTOR, NONE;
    }

    private static ClockService instance;
    protected TimeStamp curTimeStamp;
    protected String localName;

    protected ClockService() {

    };

    public static ClockService getClockSerivce(ClockType type, String localName,
                                               HashMap<String, NodeBean> nodeList) {
        if(instance != null) {
            return instance;
        }

        switch(type) {
        case LOGICAL:
            instance = new LogicalClockService(nodeList.size());
            instance.localName = localName;
            return instance;
        case VECTOR:
            instance = new VectorClockService(nodeList.size());
            instance.localName = localName;
            HashMap<String, AtomicInteger> map = (HashMap<String, AtomicInteger>) instance.getCurTimeStamp().getTimeStamp();
            // Vector time stamp need initialization
            for(Entry<String, NodeBean> cur : nodeList.entrySet()) {
                map.put(cur.getKey(), new AtomicInteger(0));
            }

            return instance;
        default:
            return null;
        }
    }

    public TimeStamp getCurTimeStamp() {
        return this.curTimeStamp;
    }

    /**
     * Update currentTimeStamp when send a message by using the step length.
     */
    public abstract void updateTimeStampOnSend();

    /**
     * Update currentTimeStamp when receive a message by comparing to timestamp
     * in the incoming message.
     * @param ts
     */
    public abstract void updateTimeStampOnReceive(TimeStamp ts);
}
