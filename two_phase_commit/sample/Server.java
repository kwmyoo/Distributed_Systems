/* 15-440 Project 4
 * Kun Woo Yoo
 * kunwooy@andrew.cmu.edu
 */

import java.util.*;
import java.io.*;
import java.util.concurrent.*;

/*
 * A class that supports recovery routine.
 * If the class is called with filename as input, continue phase 2 with
 * commit. Else, continue with abort.
 */
class RecoveryTask extends TimerTask {

    private int id;
    private boolean isCommit;
    private String filename;
    private Set<String> clients;

    public RecoveryTask(String filename, int id, Set<String> clients) {
        this.id = id;
        this.isCommit = true;
        this.clients = clients;
        this.filename = filename;
    }

    public RecoveryTask(int id, Set<String> clients) {
        this.id = id;
        this.isCommit = false;
        this.clients = clients;
    }

    @Override
    public void run() {
        File log_i = new File(String.format("log_%d", id));
        File master_log = new File("master_log");
        File tmpCollage = new File(String.format("collage_%d", id));

        Server.alertResult(true, id, log_i, clients, isCommit);

        Server.log(master_log, String.format("End %d", id));

        if (!log_i.delete()) System.err.println("deletion failed");
    }
}

class RttTask extends TimerTask {
    private int id;

    public RttTask(int id) {
        this.id = id;
    }

    @Override
    public void run() {
        Server.rttPassed.put(id, true);
    }
}

public class Server implements ProjectLib.CommitServing {
    public static long RTT = 6000; // timeout period as rtt, 6 seconds

    public static int transaction_id; // to keep track of different commits
    public static ProjectLib PL;
    // HashMap of message queues indexed by transaction id
    public static HashMap<Integer,
           LinkedBlockingQueue<ProjectLib.Message>> queueMap;
    public static HashMap<Integer, Timer> timerMap;
    public static HashMap<Integer, Boolean> rttPassed;

    // start a new thread for recovery routine after 6s
    public static void recoveryTimer(boolean isCommit, int id,
            Set<String> clients, String filename) {
        Timer timer = new Timer();
        RecoveryTask rt;
        if (isCommit) rt = new RecoveryTask(filename, id, clients);
        else rt = new RecoveryTask(id, clients);
        timer.schedule(rt, 0);
    }

    public static void rttTimer(int id) {
        Timer timer = timerMap.get(id);
        timer.schedule(new RttTask(id), RTT);
    }

    // log the given string to given file log_i. call fsync()
    public static void log(File log_i, String str) {
        try {
            FileOutputStream fos = new FileOutputStream(log_i, true);
            String appending = str.concat("\n");
            fos.write(appending.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }

        PL.fsync();
    }

    // log the initial state of a new collage, its filename and sources
    public static void logInitState(File log_i, String filename,
            String[] sources) {
        String firstLine = filename + "\n";
        StringBuilder secondLine = new StringBuilder();
        for (int i = 0; i < sources.length; i++) {
            if (i == sources.length - 1) {
                secondLine.append(sources[i]);
            } else {
                secondLine.append(sources[i] + " ");
            }
        }
        secondLine.append("\n");
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(log_i, true);
            fos.write(firstLine.getBytes());
            fos.write((secondLine.toString()).getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // for main(), add new message to appropriate queue
    public static void addMessage(ProjectLib.Message msg) {
        int id = (int)msg.body[MsgMacros.ID];
        LinkedBlockingQueue<ProjectLib.Message> tmp = queueMap.get(id);

        try { tmp.put(msg); }
        catch (Exception e) { e.printStackTrace(); }
    }

    // get next message of the calling transaction specified by id
    public static ProjectLib.Message getNextMessage(int id) {
        LinkedBlockingQueue<ProjectLib.Message> tmp = queueMap.get(id);
        ProjectLib.Message msg = null;

        try { msg = tmp.poll(); }
        catch (Exception e) { e.printStackTrace(); }
        return msg;
    }

    // parse the input sources string array and put filenames into hash table
    public static HashMap<String, ArrayList<String>>
        parseSources(String[] sources) {

        HashMap<String, ArrayList<String>> srcMap = new HashMap<>();
        String[] tmp;
        ArrayList<String> tmpArr;

        for (String info : sources) {
            tmp = info.split(":");
            tmpArr = srcMap.get(tmp[0]);

            if (tmpArr != null) {
                tmpArr.add(tmp[1]);
            } else {
                tmpArr = new ArrayList<String>();
                tmpArr.add(tmp[1]);
                srcMap.put(tmp[0], tmpArr);
            }
        }

        return srcMap;
    }

    // request users to vote for the given collage
    public static Set<String> requestVote(int curr_id, byte[] img,
            HashMap<String, ArrayList<String>> srcMap) {

        Set<String> clients = new HashSet<String>(srcMap.keySet());

        for (String client : clients) {
            ArrayList<String> files = srcMap.get(client);
            LinkedList<Byte> tmpMsg = new LinkedList<Byte>();
            tmpMsg.add((byte)curr_id);
            tmpMsg.add(MsgMacros.VOTE_REQUEST);
            tmpMsg.add((byte)files.size());

            for (String filename : files) {
                tmpMsg.add((byte)filename.length());
                for (byte b : filename.getBytes()) {
                    tmpMsg.add(new Byte(b));
                }
            }

            byte[] msg = new byte[tmpMsg.size() + img.length];
            int cnt = 0;
            for (Byte b : tmpMsg) {
                msg[cnt] = b.byteValue();
                cnt++;
            }

            System.arraycopy(img, 0, msg, cnt, img.length);
            ProjectLib.Message plmsg = new ProjectLib.Message(client, msg);
            PL.sendMessage(plmsg);
        }

        rttPassed.put(curr_id, false);
        rttTimer(curr_id);
        return clients;
    }

    /*
     * @param: clients is the set of clients to gather votes from,
     * and add clients that voted yes to the given yesClients
     *
     * @return: true if everyone voted yes, otherwise false
     */
    public static boolean gatherVote(int curr_id, Set<String> clients) {
        boolean isCommit = true;
        String client;
        byte vote;
        ProjectLib.Message plmsg = getNextMessage(curr_id);
        int clients_left = clients.size();

        while (clients_left > 0 && !rttPassed.get(curr_id)) {
            if (plmsg != null) {
                client = plmsg.addr;
                vote = plmsg.body[MsgMacros.MSG_TYPE];

                if (clients.contains(client)) {
                    if (vote == MsgMacros.VOTE_NO) {
                        isCommit = false;
                    }
                    clients_left--;
                }
            }
            plmsg = getNextMessage(curr_id);
        }

        if (clients_left > 0) isCommit = false; // if anyone timed out
        return isCommit;
    }

    // alert the result of the vote to the clients who voted yes
    // and gather acknowledges
    public static void alertResult(boolean isRec, int curr_id, File log_i,
            Set<String> clients, boolean isCommit) {
        byte[] body = new byte[2];
        body[MsgMacros.ID] = (byte)curr_id;
        if (isCommit) body[MsgMacros.MSG_TYPE] = MsgMacros.GLOBAL_COMMIT;
        else body[MsgMacros.MSG_TYPE] = MsgMacros.ABORT;
        ProjectLib.Message plmsg;

        // if this is a part of recovery process, we already sent message
        if (isRec) {
            plmsg = getNextMessage(curr_id);
            while (plmsg != null) {
                if (plmsg.body[MsgMacros.MSG_TYPE] == MsgMacros.ACK) {
                    clients.remove(plmsg.addr);
                    if (log_i != null) log(log_i, "ACK " + plmsg.addr);
                }
                plmsg = getNextMessage(curr_id);
            }
        }

        // repeat this job until all clients acknowledged
        while (!clients.isEmpty()) {
            // send result if rtt passed from last time
            if (rttPassed.get(curr_id)) {
                for (String client : clients) {
                    plmsg = new ProjectLib.Message(client, body);
                    PL.sendMessage(plmsg);
                }

                rttPassed.put(curr_id, false);
                rttTimer(curr_id);
            }

            // gather replies
            plmsg = getNextMessage(curr_id);
            while (plmsg != null) {
                if (plmsg.body[MsgMacros.MSG_TYPE] == MsgMacros.ACK) {
                    clients.remove(plmsg.addr);
                    if (log_i != null) log(log_i, "ACK " + plmsg.addr);
                }
                plmsg = getNextMessage(curr_id);
            }
        }
    }

    // save image to given filename
    public static void saveImage(String filename, byte[] img) {
        File newf = new File(filename);
        FileOutputStream fos = null;
        try {
            newf.createNewFile();
            fos = new FileOutputStream(newf);
            fos.write(img);
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * main routine for every commit
     * First parse the sources, request votes to appropriate users
     * using the parsed sources, decide YES/NO according to votes
     * and alert the result to the users who voted YES
     */
    public static void twoPhaseCommit(String filename, byte[] img,
            String[] sources) {
        int curr_id = transaction_id++;

        // get master log, create if none exists
        File master_log = null;
        try {
            master_log = new File("master_log");
            if (!master_log.exists()) {
                master_log.createNewFile();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // record current transaction number
        try {
            RandomAccessFile raf = new RandomAccessFile(master_log, "rw");
            String str_id = Integer.toString(curr_id);
            if (str_id.length() == 1) {
                str_id = "0" + str_id + "\n";
            }
            raf.write(str_id.getBytes());
            raf.close();
        } catch (Exception e) { e.printStackTrace(); }

        // save image, and create new log file for this collage
        String imageLog = String.format("collage_%d", curr_id);
        saveImage(imageLog, img);
        File tmpCollage = new File(imageLog);
        File log_i = new File(String.format("log_%d", curr_id));
        try {
            log_i.createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // log initial state and start transaction, also logging to master
        logInitState(log_i, filename, sources);
        log(master_log, String.format("Start %d", curr_id));

        LinkedBlockingQueue<ProjectLib.Message> queue =
            new LinkedBlockingQueue<>();
        Timer timer = new Timer();
        queueMap.put(curr_id, queue);
        timerMap.put(curr_id, timer);
        HashMap<String, ArrayList<String>> srcMap = parseSources(sources);
        Set<String> clients = requestVote(curr_id, img, srcMap);
        boolean isCommit = gatherVote(curr_id, clients);

        // if the decision is to commit, change filename of logged image
        if(isCommit) {
            File collage = new File(filename);
            if (!tmpCollage.renameTo(collage))
                System.err.println("renaming failed");
        // else, delete the logged image file
        } else {
            tmpCollage.delete();
        }

        // log vote decision
        log(log_i, String.format("Commit %b", isCommit));

        rttPassed.put(curr_id, true);
        alertResult(false, curr_id, log_i, clients, isCommit);

        // log end of transaction
        log(master_log, String.format("End %d", curr_id));

        if (!log_i.delete()) System.err.println("deletion failed");
    }

	public void startCommit( String filename, byte[] img, String[] sources ) {
		System.out.println( "Server: Got request to commit "+filename );
	    twoPhaseCommit(filename, img, sources);
    }

    /*
     * The recovery routine. Get unfinished transactions from master log.
     * After that, recover each transaction from each transaction's log
     * file. Send result to the clients, and start a new thread to
     * handle the transaction afterward.
     */
    public synchronized static void recoveryRoutine() throws IOException{
        Set<Integer> collages = new HashSet<Integer>();

        BufferedReader br = new BufferedReader( new FileReader("master_log"));
        String line = br.readLine();
        if (line == null) return;
        transaction_id = Integer.parseInt(line) + 1;

        line = br.readLine();
        String[] l;
        File master_log = new File("master_log");

        while (line != null) {
            l = line.split(" ");
            int id = Integer.parseInt(l[1]);
            if (l[0].equals("Start")) {
                collages.add(id);
            } else if (l[0].equals("End")) {
                collages.remove(id);
            }

            line = br.readLine();
        }

        for (Integer coll_id : collages) {
            int id = coll_id.intValue();

            LinkedBlockingQueue<ProjectLib.Message> queue =
                new LinkedBlockingQueue<>();
            Timer timer = new Timer();
            queueMap.put(id, queue);
            timerMap.put(id, timer);

            br = new BufferedReader(
                    new FileReader(String.format("log_%d",id)));
            String filename = br.readLine().trim();
            line = br.readLine().trim();

            String[] sources = line.split(" ");
            Set<String> clients = new HashSet<>();

            for (String src : sources) {
                String[] tmpl = src.split(":");
                clients.add(tmpl[0]);
            }

            boolean isCommit = false, commitWritten = false;
            line = br.readLine();
            if (line != null) {
                commitWritten = true;
                l = line.split(" ");
                if (l[1].equals("true")) isCommit = true;
                else isCommit = false;
            }

            line = br.readLine();
            while (line != null) {
                l = line.split(" ");
                clients.remove(l[1]);
                line = br.readLine();
            }

            File log_i = new File(String.format("log_%d", id));
            if (!commitWritten) log(log_i, String.format("Commit %b", isCommit));
            byte[] body = new byte[2];
            body[MsgMacros.ID] = (byte)id;
            if (isCommit) body[MsgMacros.MSG_TYPE] = MsgMacros.GLOBAL_COMMIT;
            else body[MsgMacros.MSG_TYPE] = MsgMacros.ABORT;

            ProjectLib.Message plmsg;
            for (String client : clients) {
                plmsg = new ProjectLib.Message(client, body);
                PL.sendMessage(plmsg);
            }
            rttPassed.put(id, false);
            rttTimer(id);
            recoveryTimer(isCommit, id, clients, filename);
        }
    }



	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv );
        transaction_id = 0;
        queueMap = new HashMap<>();
        timerMap = new HashMap<>();
        rttPassed = new HashMap<>();

        File master_log = new File("master_log");
        if (master_log.exists()) {
            recoveryRoutine();
        }

        // fetch messages and put it into appropriate queue
		while (true) {
            ProjectLib.Message msg = PL.getMessage();
            addMessage(msg);
		}
	}
}

