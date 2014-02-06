package distSysLab2.timeStamp;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

public class VectorTimeStamp extends TimeStamp implements Comparable<VectorTimeStamp> {
    private static final long serialVersionUID = 1L;

    private HashMap<String, AtomicInteger> localTS;

    public VectorTimeStamp(int nodeAmount) {
        localTS = new HashMap<String, AtomicInteger>(nodeAmount);
    }

    @Override
    public int compareTo(VectorTimeStamp ts) {
        boolean beforeFlag = false;
        boolean afterFlag = false;

        // For vector time stamp, we need to compare corresponding value one by one
        for(Entry<String, AtomicInteger> e : localTS.entrySet()) {
            int local = e.getValue().get();
            int remote = ts.getTimeStamp().get(e.getKey()).get();

            if(local < remote) {
                beforeFlag = true;
            }
            else if(local > remote){
                afterFlag = true;
            }
        }

        if(beforeFlag == true && afterFlag == false) {
            return -1;
        }
        else if(beforeFlag == false && afterFlag == true) {
            return 1;
        }
        else {
            return 0;
        }
    }

    @Override
    public HashMap<String, AtomicInteger> getTimeStamp() {
        return this.localTS;
    }

    @Override
    public void setTimeStamp(Object ts) {
        this.localTS = (HashMap<String, AtomicInteger>)ts;
    }

    @Override
    public String toString() {
        return localTS.toString();
    }
}
