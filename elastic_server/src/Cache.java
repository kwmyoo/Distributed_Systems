import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.rmi.registry.*;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.Naming;
import java.rmi.RemoteException;

public class Cache extends UnicastRemoteObject implements Cloud.DatabaseOps {

    public Cloud.DatabaseOps db; // original db instance
    public ConcurrentHashMap<String, String> itemMap; // our new db

    public Cache(Cloud.DatabaseOps database) throws RemoteException {
        db = database;
        itemMap = new ConcurrentHashMap<String, String>();
    }

    public String get(String key) throws RemoteException {
        String value = itemMap.get(key);
        if (value == null) {
            value = db.get(key);
            itemMap.put(key, value);
        }

        return value;
    }

    public boolean set(String key, String val, String auth) throws RemoteException{
        itemMap.put(key, val);
        return true;
    }

    public boolean transaction(String item, float price, int qty)
        throws RemoteException {
        return db.transaction(item, price, qty);
    }
}

