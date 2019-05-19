import java.io.*;

public class RequestInfo implements java.io.Serializable {
    public Cloud.FrontEndOps.Request r;
    public int vm_id;
    public int len;
    public boolean isFront;

    public RequestInfo (Cloud.FrontEndOps.Request req) {
        r = req;
    }

    public RequestInfo (boolean b) {
        isFront = b;
    }

    public RequestInfo (int vmId) {
        vm_id = vmId;
    }

    public RequestInfo(int vmId, int qlen) {
        vm_id = vmId;
        len = qlen;
    }
}
