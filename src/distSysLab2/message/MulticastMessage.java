package distSysLab2.message;

public class MulticastMessage extends TimeStampMessage {

    private static final long serialVersionUID = 1L;

    public MulticastMessage(String dest, String kind, Object data) {
        super(dest, kind, data);
    }

}
