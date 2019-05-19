import java.rmi.registry.*;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.*;

public class Server extends UnicastRemoteObject implements ServerIntf {

    // global constants
    public static int MASTER_SERVER = 1;
    public static int CACHE_SERVER = 2;
    public static int MID_SCALE = 3;
    public static int MID_MAX = 7;
    public static int FRONT_SCALE = 4;
    public static int FRONT_MAX = 3;
    public static long CHECK_INTERVAL = 300;
    public static long FRONT_CHECK = 1000;
    public static long INITIAL_BOOT = 5200;

    // global variables
    public static String cloud_ip;
    public static int cloud_port;
    public static ServerLib SL;
    public static int total_served;
    public static long start_time;
    public static int vm_count;
    public static int front_count;
    public static int mid_count;
    public static int max_id;
    private static final long serialVersionUID = 1L;

    // global data structures
    public static Cache cache;
    public static ArrayList<Integer> frontArr;
    public static ArrayList<Integer> midArr;
    public static int queueLength[];

    public static int[] hourMid = {2, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2,
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2}; // base number of mid-tier for given time
    public static int[] hourFront = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}; // base number of front-end servers

    public static double[] rateArr = {0.0, 0.00012, 0.00052, 0.00125, 0.00165,
        0.00225, 0.00325, 0.00425, 0.00625, 0.00825}; // request rate array
    public static int[] numMid = {1, 1, 2, 2, 2, 4, 5, 6, 9, 9}; // number to add
    public static int[] numFront = {1, 1, 1, 1, 1, 2, 2, 2, 3, 3};

    public Server() throws RemoteException {
        super(0);
    }

    // get the front end server with longest queue
    public int getBusiest() throws RemoteException {
        /*
        int len = queueLength.length;
        int max_val = queueLength[0];
        int max_id = 0;
        for (int i = 1; i < len; i++) {
            if (queueLength[i] > max_val) {
                max_val = queueLength[i];
                max_id = i;
            }
        } */

        int vm_id = frontArr.get(max_id);
        if (max_id == (front_count - 1)) {
            max_id = 0;
        } else max_id++;

        return vm_id;
    }

    // fetch next request of this server
    public RequestInfo fetchNextRequest() throws RemoteException  {
        RequestInfo reqInfo = new RequestInfo(SL.getNextRequest());
        return reqInfo;
    }

    // set queue length of this server in master server
    public void setQueueLength(RequestInfo reqInfo) throws RemoteException{
        int i = frontArr.indexOf(reqInfo.vm_id);
        queueLength[i] = reqInfo.len;
    }

    public boolean checkFront(int vm_id) throws RemoteException {
        if (frontArr.indexOf(vm_id) >= 0)
            return true;
        else
            return false;
    }

    public void terminateVM() throws RemoteException {
        SL.shutDown();
    }

    public static void generateVM(int fNum, int mNum) {
        if (fNum < 0 || mNum < 0) return;
        int new_len = queueLength.length + fNum;
        int[] tempArr = new int[new_len];
        int i;

        for (i = 0; i < new_len; i++) {
            if (i < queueLength.length) {
                tempArr[i] = queueLength[i];
            } else {
                tempArr[i] = -1;
            }
        }

        queueLength = tempArr;
        for (i = 0; i < fNum; i++) {
            frontArr.add(vm_count);
            SL.startVM();
            vm_count++;
            front_count++;
        }
        for (i = 0; i < mNum; i++) {
            midArr.add(vm_count);
            SL.startVM();
            vm_count++;
            mid_count++;
        }
    }

    public static void removeVM(int fNum, int mNum) {
        if (fNum < 0 || mNum < 0) return;
        int new_len = queueLength.length - fNum;
        int[] tempArr = new int[new_len];
        int i;

        for (i = 0; i < new_len; i++) {
            tempArr[i] = queueLength[i];
        }
        Registry registry = null;
        ServerIntf target = null;
        String socket = null;
        int target_id;

        for (i = 0; i < fNum; i++) {
            target_id = frontArr.get(front_count - 1);

            try {
                registry = LocateRegistry.getRegistry(cloud_port);
                socket = String.format("//%s:%d/FrontEnd%d", cloud_ip, cloud_port,
                        target_id);
                target = (ServerIntf)registry.lookup(socket);
                target.terminateVM();
                frontArr.remove(front_count - 1);
            } catch (Exception e) {
                e.printStackTrace();
            }
            SL.endVM(front_count - i);
            front_count--;
        }

        for (i = 0; i < mNum; i++) {
            target_id = midArr.get(mid_count - 1);
            SL.endVM(vm_count - 1 - i);
            midArr.remove(mid_count - 1);
            mid_count--;
        }
        queueLength = tempArr;
    }

    public static int neededIndex(double rate) {
        int i;
        for (i = 1; i < rateArr.length; i++) {
            if (rate < rateArr[i]) {
                break;
            }
        }
        return i-1;
    }

    public static void main ( String args[] ) throws Exception {
        start_time = System.currentTimeMillis();
		if (args.length != 3) throw new Exception("Need 3 args: <cloud_ip> <cloud_port> <VM id>");
        long end = 0;
        long new_end = 0;
        long diff = 0;
        int vm_id = Integer.parseInt(args[2]);
        cloud_ip = args[0];
        cloud_port = Integer.parseInt(args[1]);
        SL = new ServerLib( args[0], cloud_port );
        Cloud.FrontEndOps.Request r = null;

        if (vm_id == MASTER_SERVER) {
            SL.register_frontend();
            r = SL.getNextRequest();
            end = System.currentTimeMillis();

            if ((end - start_time) < 250L) {
                SL.drop(r);
                r = SL.getNextRequest();
                new_end = System.currentTimeMillis();
                diff = new_end - end;
            } else {
                diff = end - start_time;
            }
            SL.startVM();
            vm_count = 3;
        }

        boolean isFront = true;
        long last_time = start_time;
        long curr_time;
        Registry registry = null;
        ServerIntf master = null;
        try {
            registry = LocateRegistry.getRegistry(cloud_port);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (vm_id == MASTER_SERVER) { // master server, also a front end server

            SL.drop(r);

            int currentHour = (int)SL.getTime();
            int numFrontEnd = hourFront[currentHour] + 1; // including master server 1
            int numMidTier = hourMid[currentHour];
            frontArr = new ArrayList<Integer>();
            frontArr.add(1);
            midArr = new ArrayList<Integer>();
            queueLength = new int[1];
            front_count = 1;
            mid_count = 0;
            total_served = 0;
            generateVM(numFrontEnd-1, numMidTier);

            Server srv = null;
            try {
                srv = new Server();
                registry.bind(String.format("//%s:%d/FrontEnd%d", cloud_ip,
                            cloud_port, vm_id), srv);
            } catch (Exception e) {
                e.printStackTrace();
            }
            int max_id = 0;
            int mid_threshold;
            int ni = 0;
            long time_passed;
            double request_rate;
            boolean scaleIn = false;
            boolean isFrontEnd = true;
            while (true) {
                curr_time = System.currentTimeMillis();
                time_passed = (int)(curr_time - start_time);
                if (isFrontEnd) {
                    if (time_passed < INITIAL_BOOT) {
                        r = SL.getNextRequest();
                        SL.processRequest(r);
                        if (total_served == 0) {
                            System.err.println(diff);
                            request_rate = 1 / (double)(diff);
                            ni = neededIndex(request_rate);
                            generateVM(numFront[ni] - front_count, numMid[ni] - mid_count);
                            if ((numFront[ni] < front_count) ||
                                    (numMid[ni] < mid_count)) {
                                scaleIn = true;
                            }
                        }
                        total_served++;
                    }
                    else if (scaleIn && (time_passed > INITIAL_BOOT)) {
                        scaleIn = false;
                        removeVM(front_count - numFront[ni], mid_count - numMid[ni]);
                    }
                    else {
                        isFrontEnd = false;
                       // SL.unregister_frontend();
                       // queueLength[0] = -1;
                    }
                }
                else {
/*                    if (curr_time > (last_time + CHECK_INTERVAL)) {
                        queueLength[0] = SL.getQueueLength();
                        if (mid_count == 3) {
                            mid_threshold = 6;
                        } else if ((mid_count == 4 || (mid_count == 5))) {
                            mid_threshold = 8;
                        } else {
                            mid_threshold = MID_SCALE * mid_count;
                        }
                        if ((queueLength[0] >= mid_threshold) &&
                                (queueLength[0] < MID_MAX)) {
                            generateVM(0, 1);
                        }  else if ((front_count < FRONT_MAX) &&
                                (queueLength[0] >= (FRONT_SCALE * front_count))) {
                            generateVM(1, 0);
                        }
                        last_time = curr_time;
                    }*/
                }
            }
        } else if (vm_id == CACHE_SERVER) { // if cache server
            cache = new Cache(SL.getDB());
            registry.bind(String.format("//%s:%d/Cache", cloud_ip, cloud_port), cache);
            while (true) {}

        } else { // if not master server, find out its role
            master = (ServerIntf)registry.lookup(String.format(
                    "//%s:%d/FrontEnd1", cloud_ip, cloud_port));
            isFront = master.checkFront(vm_id);
        }

        if (isFront) { // front end servers
            System.err.println("Front end started");
            Server srv = null;
            SL.register_frontend();
            try {
                // register itself as a front end server
                srv = new Server();
                registry.bind(String.format("//%s:%d/FrontEnd%d", args[0],
                            cloud_port, vm_id), srv);
            } catch (Exception e) {
                e.printStackTrace();
            }
            int qlen;
            while (true) {
//                curr_time = System.currentTimeMillis();
  //              if (curr_time > (last_time + FRONT_CHECK)) {
    //                qlen = SL.getQueueLength();
      //              RequestInfo reqInfo = new RequestInfo(vm_id, qlen);
        //            master.setQueueLength(reqInfo);
          //          last_time = curr_time;
            //    }
            }
        } else { // middle tier servers
            System.err.println("middle tier started");
            ServerIntf target = null;

            Cloud.DatabaseOps db = null;
            boolean cacheNotFound = true;
            while (cacheNotFound) {
                try {
                    db = (Cloud.DatabaseOps)registry.lookup(
                            String.format("//%s:%d/Cache", cloud_ip, cloud_port));
                    cacheNotFound = false;
                } catch (Exception e) {
                }
            }
            int target_id;
            String socket;
            RequestInfo reqInfo;
            while (true) {
                target_id = master.getBusiest();
                try {
                    socket = String.format("//%s:%d/FrontEnd%d", args[0],
                            cloud_port, target_id);
                    target = (ServerIntf)registry.lookup(socket);
                    reqInfo = target.fetchNextRequest();
                    SL.processRequest(reqInfo.r);
                } catch (Exception e) {}
            }
        }
	}
}

