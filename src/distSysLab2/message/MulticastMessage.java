package distSysLab2.message;

public class MulticastMessage extends TimeStampMessage {

    private static final long serialVersionUID = 1L;
    
    private String srcGroup;
    private int num;
    

    public MulticastMessage(String dest, String kind, Object data) {
        super(dest, kind, data);
    }
    
    public static MulticastMessage Multicast(TimeStampMessage msg) {
        MulticastMessage newMsg = new MulticastMessage(msg.getDest(), msg.getKind(), msg.getData());
        newMsg.setDuplicate(msg.getDuplicate());
        newMsg.setSeqNum(msg.getSeqNum());
        newMsg.setSrc(msg.getSrc());
        newMsg.setTimeStamp(msg.getTimeStamp());
        return newMsg;
    }


	public String getSrcGroup() {
		return srcGroup;
	}


	public void setSrcGroup(String srcGroup) {
		this.srcGroup = srcGroup;
	}


	public int getNum() {
		return num;
	}


	public void setNum(int num) {
		this.num = num;
	}

}
