package distSysLab2.clock;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import distSysLab2.timeStamp.TimeStamp;
import distSysLab2.timeStamp.VectorTimeStamp;

public class VectorClockService extends ClockService {
    public VectorClockService(int nodeAmount) {
        this.curTimeStamp = new VectorTimeStamp(nodeAmount);
    }

    @Override
    public void updateTimeStampOnSend() {
        VectorTimeStamp cur = (VectorTimeStamp) getCurTimeStamp();
        cur.getTimeStamp().put(localName, new AtomicInteger(cur.getTimeStamp().get(localName).get() + step));
    }

    @Override
    public void updateTimeStampOnReceive(TimeStamp ts) {
        VectorTimeStamp local = (VectorTimeStamp) getCurTimeStamp();
        VectorTimeStamp remote = (VectorTimeStamp) ts;
        HashMap<String, AtomicInteger> localMap = local.getTimeStamp();
        HashMap<String, AtomicInteger> remoteMap = remote.getTimeStamp();

        for(Entry<String, AtomicInteger> e : localMap.entrySet()) {
            int localVal = e.getValue().get();
            int remoteVal = remoteMap.get(e.getKey()).get();
            // Update each value in the vector by a "max plus one" manner.
            localMap.put(e.getKey(), new AtomicInteger(localVal < remoteVal ? remoteVal : localVal));
        }

        localMap.put(localName, new AtomicInteger(localMap.get(localName).get() + step));
    }
}
