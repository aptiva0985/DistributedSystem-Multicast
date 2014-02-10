package distSysLab2.message;

import java.io.Serializable;

public class MessageKey implements Serializable {
    private static final long serialVersionUID = 1L;
    
    String destGroup;
    String src;
    int num;
    
    public String getDestGroup() {
        return destGroup;
    }
    
    public void setDestGroup(String destGroup) {
        this.destGroup = destGroup;
    }
    
    public String getSrc() {
        return src;
    }
    
    public void setSrc(String src) {
        this.src = src;
    }
    
    public int getNum() {
        return num;
    }
    
    public void setNum(int num) {
        this.num = num;
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MessageKey))
            return false;
        else
            return ((MessageKey) o).destGroup.equals(this.destGroup) &&
                   ((MessageKey) o).src.equals(this.src) &&
                   ((MessageKey) o).num == this.num;
    }
    
    @Override
    public int hashCode() {
        int part1 = destGroup.hashCode();
        int part2 = src.hashCode();
        
        return part1 | part2 | num;
    }
    
    @Override
    public String toString() {
        return "destGroup: " + destGroup + " src: " + src + " num: " + num;
    }
}
