package distSysLab2.message;

import distSysLab2.timeStamp.LogicalTimeStamp;
import distSysLab2.timeStamp.TimeStamp;
import distSysLab2.timeStamp.VectorTimeStamp;

public class TimeStampMessage extends Message implements Comparable<TimeStampMessage>{
    private static final long serialVersionUID = 1L;

    private TimeStamp timeStamp;

    public TimeStampMessage(String dest, String kind, Object data) {
        super(dest, kind, data);
    }

    public void setTimeStamp(TimeStamp timeStamp) {
        this.timeStamp = timeStamp;
    }

    public TimeStamp getTimeStamp() {
        return this.timeStamp;
    }

    @Override
    public TimeStampMessage copyOf() {
        TimeStampMessage to = new TimeStampMessage(this.dest, this.kind, this.data);
        to.duplicate = this.duplicate;
        to.seqNum = this.seqNum;
        to.src = this.src;
        to.timeStamp = this.timeStamp;

        return to;
    }

    @Override
    public int compareTo(TimeStampMessage o) {
        TimeStamp stamp = o.getTimeStamp();
        if (stamp instanceof LogicalTimeStamp) {
            return ((LogicalTimeStamp)this.timeStamp).compareTo((LogicalTimeStamp)stamp);
        }
        else if (stamp instanceof VectorTimeStamp) {
            return ((VectorTimeStamp)this.timeStamp).compareTo((VectorTimeStamp)stamp);
        }

        return 0;
    }

    @Override
    public String toString() {
        return "From:" + this.getSrc() + " to:" + this.getDest() +
               " Seq:" + this.getSeqNum() + " Kind:" + this.getKind()
               + " Dup:" + this.getDuplicate() + " TimeStamp: " + this.getTimeStamp()
               + " [Data:" + this.getData() + " ]";
    }
}
