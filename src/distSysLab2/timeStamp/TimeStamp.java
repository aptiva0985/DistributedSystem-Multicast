package distSysLab2.timeStamp;

import java.io.Serializable;

public abstract class TimeStamp implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Set time stamp to a current value
     */
    abstract public void setTimeStamp(Object o);

    /**
     * Get the current time stamp.
     * @return The current time stamp.
     */
    abstract public Object getTimeStamp();
}
