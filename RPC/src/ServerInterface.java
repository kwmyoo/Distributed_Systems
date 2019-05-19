import java.rmi.*;
import java.io.*;

public interface ServerInterface extends Remote {
    public int setVersionRemote(FileInfo fi) throws RemoteException;
    public FileInfo checkFile(FileInfo fi) throws RemoteException;
    public long fileLength(FileInfo fi) throws RemoteException;
    public boolean createFile(FileInfo fi) throws RemoteException;
    public byte[] readFile(FileInfo fi) throws RemoteException;
    public long writeFile(FileInfo fi) throws RemoteException;
    public boolean unlinkFile(FileInfo fi) throws RemoteException;
}
