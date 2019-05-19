
import java.io.*;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.SecurityManager;
import static java.lang.System.out;
import java.net.MalformedURLException;
import java.rmi.registry.*;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.Naming;
import java.rmi.RemoteException;

public class Server extends UnicastRemoteObject implements ServerInterface {

    public static String root;
    public static HashMap<String, Integer> versions;

    private static final long serialVersionUID = 1L;

    public Server() throws RemoteException{
        versions = new HashMap<String, Integer>();
    }



    String createAbsPath(String path) {
        StringBuilder pathSB = new StringBuilder(root);
        if (root.charAt(root.length() - 1) != '/') pathSB.append("/");
        pathSB.append(path);
        return pathSB.toString();
    }

    public int setVersionRemote(FileInfo fi) throws RemoteException{
        String path = createAbsPath(fi.path);
        versions.put(path, versions.get(path) + 1);
        return versions.get(path);
    }

    // @return: -2 if is directory, -1 if nonexistent, else version number
    public FileInfo checkFile(FileInfo fi) throws RemoteException {
        String path = createAbsPath(fi.path);
        File newfile = new File(path);

        if (!newfile.exists()) {
            fi.offset = -1;
            return fi;
        }
        if (newfile.isDirectory()) {
            fi.offset = -2;
            return fi;
        }

        if (versions.containsKey(path)) {

            fi.offset = versions.get(path);
        }
        else {
            versions.put(path, 0);
            fi.offset = 0;
        }
        fi.length = (int)newfile.length();
        return fi;
    }

    public long fileLength(FileInfo fi) throws RemoteException {
        String path = createAbsPath(fi.path);
        File newfile = new File(path);
        long len;
        try{
            len = newfile.length();
        } catch (SecurityException e) {
            return -1;
        }

        return len;
    }

    public synchronized boolean createFile(FileInfo fi) throws RemoteException {
        String path = createAbsPath(fi.path);
        File newfile = new File(path);

        if (newfile.isFile()) return true;

        try{
            newfile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        versions.put(path, 0);
        return true;
    }

    public byte[] readFile(FileInfo fi) throws RemoteException {
        String path = createAbsPath(fi.path);
        File newfile = new File(path);
        byte[] buf = new byte[(int)fi.length];


        try{
            RandomAccessFile ranfile = new RandomAccessFile(newfile, "r");
            ranfile.seek(fi.offset);
            ranfile.read(buf);
            ranfile.close();
        } catch (FileNotFoundException e) {
            return null;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buf;
    }


    public synchronized long writeFile(FileInfo fi) throws RemoteException{
        String path = createAbsPath(fi.path);

        File newfile = new File(path);

        try{
            RandomAccessFile ranfile = new RandomAccessFile(newfile, "rw");
            ranfile.seek(fi.offset);
            ranfile.write(fi.buf);
            ranfile.close();
        } catch (FileNotFoundException e) {
            return -1;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }

        return fi.length;
    }

    public synchronized boolean unlinkFile(FileInfo fi) throws RemoteException{
        String path = createAbsPath(fi.path);
        File newfile = new File(path);

        if (newfile.isDirectory()) {
            return false;
        }
        if (!newfile.isFile()) {
            return false;
        }
        try {
            newfile.delete();
        } catch (SecurityException e) {
            return false;
        }

        if (versions.containsKey(path)) versions.remove(path);

        return true;
    }

	public static void main(String[] args) {
        root = args[1];
        int port = Integer.parseInt(args[0]);
        try{
            LocateRegistry.createRegistry(port);
        } catch (RemoteException e) {
            System.err.println("createRegistry failure");
        }

        Server server = null;
        try{
            server = new Server();
        } catch (RemoteException e) {
            System.err.println("remote exception");
            System.exit(1);
        }
        try{
            Naming.rebind(String.format("//127.0.0.1:%d/ServerService", port), server);
        } catch (RemoteException e) {
            System.err.println("remote exception");
        } catch (MalformedURLException e) {
            System.err.println("malformed url exception");
        }
	}
}

