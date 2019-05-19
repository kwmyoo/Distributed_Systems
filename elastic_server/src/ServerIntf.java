import java.rmi.*;

public interface ServerIntf extends Remote {
    public Cloud.FrontEndOps.Request fetchNextRequest() throws RemoteException;
    public void addQueue(Cloud.FrontEndOps.Request r) throws RemoteException;
    public void terminateVM() throws RemoteException;
    public boolean checkFront(int vm_id) throws RemoteException;
    public void alertScaleIn(boolean isFront) throws RemoteException;
}
