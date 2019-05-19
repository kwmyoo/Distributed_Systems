import java.rmi.registry.*;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;

public class Server extends UnicastRemoteObject implements ServerIntf {

    // global macros
    public static int MASTER_SERVER = 1; // master server vm id
    public static int CACHE_SERVER = 2; // cache server vm id
    public static int MID_SCALE = 2;
    public static int MID_MAX = 8; // max number of mid tier
    public static int FRONT_SCALE = 4;
    public static int FRONT_MAX = 3; // max number of front
    public static long CHECK_INTERVAL = 100; // check for scaling
    public static long INITIAL_BOOT = 5200; // initial booting time
    public static long MIN_THRESHOLD = 500;
    public static int MID_TIMEOUT_CNT = 3; // shutdown after this count: mid
    public static int TIMEOUT = 300; // timeout limit for fetch request
    public static int MID_FIRST = 2; // first adjustment threshold
    public static int MID_SECOND = 4; // second adjustment
    public static int MID_THIRD = 6;
    public static int FRONT_FIRST = 2;
    public static long FRONT_TIMEOUT = 600;

    // global variables
    public static String cloud_ip;
    public static int cloud_port;
    public static ServerLib SL;
    public static int total_served;
    public static long start_time;
    public static int vm_count;
    public static int front_count;
    public static int mid_count;
    public static boolean scale_in = false;
    public static boolean scale_out = false;
    private static final long serialVersionUID = 1L;

    // global data structures
    public static Cache cache;
    public static ArrayList<Integer> frontArr; // array of front
    public static ArrayList<Integer> midArr; // array of mid
    // request queue managed by the master server
    public static LinkedBlockingQueue<Cloud.FrontEndOps.Request> reqQueue;

    public static int[] hourMid = {1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 2, 2,
        2, 1, 1, 1, 2, 2, 2, 2, 2, 1}; // base number of mid-tier for given time
    public static int[] hourFront = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}; // base number of front-end servers
    public static double[] rateArr = {0.0, 0.0009, 0.0025, 0.0031, 0.0041,
        0.0075}; // request rate array
    public static int[] numMid = {1, 2, 3, 3, 6, 8}; // number to add
    public static int[] numFront = {1, 1, 1, 2, 2, 3};

    public Server() throws RemoteException {
        super(0);
    }

    // for master: add request to the queue
    public void addQueue(Cloud.FrontEndOps.Request r) throws RemoteException {
        reqQueue.add(r);
    }

    // for master: scale in
    // @param: isFront indicates whether to scale in front or mid
    public void alertScaleIn(boolean isFront) throws RemoteException {
        scale_in = true;
        if (!scale_out) {
            if (isFront && (front_count > 1))
                removeVM(1,0);
            else if ((!isFront) && (mid_count > 1)) {
                removeVM(0,1);
            }
        }
    }

    // fetch next request from master queue
    public Cloud.FrontEndOps.Request fetchNextRequest() throws RemoteException  {
        Cloud.FrontEndOps.Request r = null;
        try {
            r = reqQueue.poll(TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return r;
    }

    // check whether this server is master or not
    public boolean checkFront(int vm_id) throws RemoteException {
        if (frontArr.indexOf(vm_id) >= 0)
            return true;
        else
            return false;
    }

    // terminate this VM
    public void terminateVM() throws RemoteException {
        SL.shutDown();
    }

    // generate fNum front tier and mNum mid tier VM's
    public static void generateVM(int fNum, int mNum) {
        if (fNum < 0 || mNum < 0) return;
        int i;
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

    // remove fNum frontier and mNum mid tier VMs
    public static void removeVM(int fNum, int mNum) {
        if (fNum < 0 || mNum < 0) return;
        int i;

        Registry registry = null;
        ServerIntf target = null;
        String socket = null;
        int target_id;

        for (i = 0; i < fNum; i++) {
            target_id = frontArr.get(front_count - 1);
            if (target_id == MASTER_SERVER) return;
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
            SL.endVM(target_id);
            front_count--;
        }

        for (i = 0; i < mNum; i++) {
            target_id = midArr.get(mid_count - 1);
            SL.endVM(target_id);
            midArr.remove(mid_count - 1);
            mid_count--;
        }
    }

    // calculate the index (for numFront and numMid array) to adjust VM number
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
		if (args.length != 3) throw new Exception(
                "Need 3 args: <cloud_ip> <cloud_port> <VM id>");
        long end = 0;
        long new_end = 0;
        long diff = 0;
        int vm_id = Integer.parseInt(args[2]);
        cloud_ip = args[0];
        cloud_port = Integer.parseInt(args[1]);
        SL = new ServerLib( args[0], cloud_port );
        Cloud.FrontEndOps.Request r = null;

        // calculate request rate based on first arrival
        if (vm_id == MASTER_SERVER) {
            SL.register_frontend();
            r = SL.getNextRequest();
            end = System.currentTimeMillis();

            // if less than threshold, calculate agian for precision
            if ((end - start_time) < MIN_THRESHOLD) {
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

        boolean isFront = true; // for non-masters: checked later
        long last_time = start_time;
        long curr_time;
        Registry registry = null;
        ServerIntf master = null;
        try {
            registry = LocateRegistry.getRegistry(cloud_port);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // master server routine (also a front)
        if (vm_id == MASTER_SERVER) {
            SL.drop(r);

            int currentHour = (int)SL.getTime();
            int numFrontEnd = hourFront[currentHour] + 1;
            int numMidTier = hourMid[currentHour];
            frontArr = new ArrayList<Integer>();
            frontArr.add(1);
            midArr = new ArrayList<Integer>();
            front_count = 1;
            mid_count = 0;
            total_served = 0;
            generateVM(numFrontEnd-1, numMidTier); // generate vm based on time

            Server srv = null;
            try {
                srv = new Server();
                registry.bind(String.format("//%s:%d/FrontEnd%d", cloud_ip,
                            cloud_port, vm_id), srv);
            } catch (Exception e) {
                e.printStackTrace();
            }

            // create request queue and declare variables
            reqQueue = new LinkedBlockingQueue<Cloud.FrontEndOps.Request>();
            int max_id = 0;
            int mid_threshold, front_threshold, qlen;
            int ni = 0;
            long time_passed;
            double request_rate;
            boolean scaleIn = false;
            boolean isMid = true;
            // enter main loop
            while (true) {
                curr_time = System.currentTimeMillis();
                time_passed = (int)(curr_time - start_time);
                if (isMid) { // before initial boot, act as mid tier
                    if (time_passed < INITIAL_BOOT) {
                        r = SL.getNextRequest();
                        SL.processRequest(r);

                        // adjust number of VM's based on request rate
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
                    else { // if past initial boot, only act as front
                        isMid = false;
                    }
                }
                else {
                    r = SL.getNextRequest();
                    reqQueue.add(r);

                    // check for scaling every CHECK_INTERVAL
                    if (!scale_in &&
                            (curr_time > (last_time + CHECK_INTERVAL))) {
                        qlen = SL.getQueueLength();
                        mid_threshold = MID_SCALE * mid_count;
                        if (mid_count >= MID_THIRD) {
                            mid_threshold = mid_count - 2;
                        } else if (mid_count >= MID_SECOND) {
                            mid_threshold = mid_count - 1;
                        } else if (mid_count == MID_FIRST) {
                            mid_threshold = (MID_SCALE - 1) * mid_count;
                        } else {
                            mid_threshold = MID_SCALE * mid_count;
                        }

                        if (front_count == FRONT_FIRST) {
                            front_threshold = FRONT_SCALE + 1;
                        } else {
                            front_threshold = FRONT_SCALE * front_count;
                        }

                        if ((qlen >= mid_threshold) &&
                                (mid_count < MID_MAX)) {
                            generateVM(0, 1);
                            scale_out = true;
                        }
                        if ((front_count < FRONT_MAX) &&
                                (qlen >= front_threshold)) {
                            generateVM(1, 0);
                            scale_out = true;
                        }
                        last_time = curr_time;
                    }
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
        // front tier servers
        if (isFront) {
            System.err.println("front end started");
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
            int timeouts = 0;
            RequestInfo reqInfo;
            long start = System.currentTimeMillis();
            while (true) {
                r = SL.getNextRequest();
                master.addQueue(r);

                end = System.currentTimeMillis();
                if ((end - start) > FRONT_TIMEOUT) {
                    timeouts++;
                    if (timeouts == 2) {
                        master.alertScaleIn(true);
                        timeouts = 0;
                    }
                } else {
                    timeouts = 0;
                }
                start = end;
            }
        }
        // middle tier servers
        else {
            System.err.println("middle tier started");
            ServerIntf target = null;

            Cloud.DatabaseOps db = null;
            boolean cacheNotFound = true;

            // find cache by looping until it registers
            while (cacheNotFound) {
                try {
                    db = (Cloud.DatabaseOps)registry.lookup(
                            String.format("//%s:%d/Cache", cloud_ip, cloud_port));
                    cacheNotFound = false;
                } catch (Exception e) {
                }
            }

            // routine: process request
            int target_id;
            int timeouts = 0;
            String socket;
            while (true) {
                try{
                    r = master.fetchNextRequest();
                } catch (Exception e) {}
                if (r != null) {
                    SL.processRequest(r, db);
                    timeouts = 0;
                } else {
                    timeouts++;
                    if (timeouts == MID_TIMEOUT_CNT) {
                        master.alertScaleIn(false);
                        timeouts = 0;
                    }
                }
            }
        }
	}
}

