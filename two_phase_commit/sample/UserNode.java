/* Skeleton code for UserNode */
import java.util.*;
import java.io.*;
import java.util.concurrent.*;

public class UserNode implements ProjectLib.MessageHandling {
	public final String myId;
    public static ProjectLib PL;
    public static HashSet<String> usedFiles;
    public static HashSet<Integer> blockedTransaction;
    public static HashMap<Integer, LinkedBlockingQueue<ProjectLib.Message>> queueMap;
    public static HashMap<Integer, String[]> needRecovery;

    public UserNode( String id ) {
		myId = id;
	}

    /*
     * This method will only be called when client voted "yes" and waits for
     * reply. Hence, it needs to block until a message comes
     */
    public static ProjectLib.Message getNextMessage(int id) {
        LinkedBlockingQueue<ProjectLib.Message> queue =
            queueMap.get(id);
        ProjectLib.Message msg = null;
        try {
            msg = queue.take();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return msg;
    }

    // add the new message to its transaction's message queue
    public static void addMessage(ProjectLib.Message msg) {
        int transaction_id = (int)msg.body[MsgMacros.ID];
        LinkedBlockingQueue<ProjectLib.Message> queue =
            queueMap.get(transaction_id);
        try {
            queue.add(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * Send the result of whether or not this user node will commit or
     * not to the server. If the decision is to commit, block until
     * a reply comes from the server, and act accordingly. Else, mark
     * the files as unused and return.
     */
    public void clientRoutine(int id, boolean userCommit,
            String[] files, String addr) {

        byte[] body = new byte[2];
        body[MsgMacros.ID] = (byte)id;

        File master_log = null;
        FileOutputStream fos = null;
        if (userCommit) {
            blockedTransaction.add(id); // if yes, we need reply from server
            body[MsgMacros.MSG_TYPE] = MsgMacros.VOTE_YES;
            StringBuilder sb = new StringBuilder(String.format("Block %d", id));
            for (String filename : files) {
                sb.append(" " + filename);
                usedFiles.add(filename);
            }
            sb.append("\n");

            String log_str = sb.toString();
            try {
                master_log = new File("master_log");
                if (!master_log.exists()) master_log.createNewFile();
                fos = new FileOutputStream(master_log, true);
                fos.write(log_str.getBytes());
                PL.fsync();
            } catch (Exception e) { e.printStackTrace(); }

        }
        else body[MsgMacros.MSG_TYPE] = MsgMacros.VOTE_NO;

        // send this user node's decision
        ProjectLib.Message plmsg = new ProjectLib.Message(addr, body);
        PL.sendMessage(plmsg);

        // if commit, wait for response and act after
        if(userCommit) {
            File target;
            plmsg = getNextMessage(id);

            if (plmsg.body[MsgMacros.MSG_TYPE] == MsgMacros.GLOBAL_COMMIT) {
                for (String filename : files) {
                    target = new File(filename);
                    target.delete(); // delete source file if commit
                }
            } else if (plmsg.body[MsgMacros.MSG_TYPE] == MsgMacros.ABORT) {
                // if server says abort, mark files as unused
                for (String filename : files) {
                    usedFiles.remove(filename);
                }
            }

            String new_line = String.format("Unblock %d\n", id);
            try{
                fos.write(new_line.getBytes());
                PL.fsync();
                fos.close();
            } catch (Exception e) { e.printStackTrace(); }

            body[1] = MsgMacros.ACK;
            plmsg = new ProjectLib.Message(plmsg.addr, body);
            PL.sendMessage(plmsg);
            blockedTransaction.remove(id); // unblock transaction
        }
        System.err.println(String.format("Client %s ended normally", myId));
    }

    /*
     * @param: body is the message of body, and files is out-param
     * to return the array of filenames for this client
     * @return: true if user wants commit, false otherwise
     */
     public static boolean processRequest(byte[] body, String[] files) {
        int num_files = (int)body[MsgMacros.NUM_FILES];
        int cnt = 3, strlen;
        String tmp;
        boolean used = false;
        for (int i = 0; i < num_files; i++) {
            strlen = (int)body[cnt];
            tmp = new String(body, cnt+1, strlen);
            files[i] = tmp;
            if (usedFiles.contains(tmp)) used = true;

            cnt += (1 + strlen);
        }
        if (used) return false;

        byte[] img = new byte[body.length - cnt];
        System.arraycopy(body, cnt, img, 0, img.length);

        return PL.askUser(img, files);
    }

    // @param: the input message is related to a previously blocked
    // transaction because it voted yes, but failed afterwards
    public void recoverTransaction(ProjectLib.Message msg) {
        int id = (int)msg.body[MsgMacros.ID];
        String[] files = needRecovery.get(id);

        // if commit
        if (msg.body[MsgMacros.MSG_TYPE] == MsgMacros.GLOBAL_COMMIT) {
            for (String filename : files) {
                File target = new File(filename);
                target.delete(); // delete source file if commit
            }
        } else if (msg.body[MsgMacros.MSG_TYPE] == MsgMacros.ABORT) {
            // if server says abort, mark files as unused
            for (String filename : files) {
                usedFiles.remove(filename);
            }
        }

        File master_log = null;
        FileOutputStream fos = null;
        String new_line = String.format("Unblock %d\n", id);
        try{
            master_log = new File("master_log");
            fos = new FileOutputStream(master_log);
            fos.write(new_line.getBytes());
            PL.fsync();
            fos.close();
        } catch (Exception e) { e.printStackTrace(); }

        needRecovery.remove(id);
        byte[] body = new byte[2];
        body[MsgMacros.ID] = (byte)id;
        body[MsgMacros.MSG_TYPE] = MsgMacros.ACK;
        ProjectLib.Message plmsg = new ProjectLib.Message(msg.addr, body);
        PL.sendMessage(plmsg);
        blockedTransaction.remove(id); // unblock transaction

        System.err.println(String.format("Client %s recovered", myId));
    }

    /*
     * When a new message arrives, this method checks whether it is about
     * a new commit or not. If it is, enter main routine and create
     * new message queue for the given transaction id. If not, add the
     * message to appropriate queue
     */
	public boolean deliverMessage( ProjectLib.Message msg ) {
        // if the message is about new commit, enter routine
        if (msg.body[1] == MsgMacros.VOTE_REQUEST) {
            int id = msg.body[MsgMacros.ID];
            LinkedBlockingQueue<ProjectLib.Message> queue =
                new LinkedBlockingQueue<>();
            queueMap.put(id, queue);

            String[] files = new String[(int)msg.body[MsgMacros.NUM_FILES]];
            boolean userCommit = processRequest(msg.body, files);
            clientRoutine(id, userCommit, files, msg.addr);

        // if the message is not about new commit
        } else {
            int transaction_id = (int)msg.body[MsgMacros.ID];

            // if the specified transaction is currently blocked
            if (blockedTransaction.contains(transaction_id)) {
                addMessage(msg);

            // if the transaction was blocked before a failure
            } else if (needRecovery.containsKey(transaction_id)) {
                recoverTransaction(msg);

            // if not, just reply with ACK
            } else {
                byte[] body = new byte[2];
                body[MsgMacros.ID] = (byte)transaction_id;
                body[MsgMacros.MSG_TYPE] = MsgMacros.ACK;
                ProjectLib.Message plmsg =
                    new ProjectLib.Message(msg.addr, body);
                PL.sendMessage(plmsg);
            }
        }

        return true;
	}

    public synchronized static void recoveryRoutine() throws IOException {
        File master_log = new File("master_log");
        BufferedReader br = new BufferedReader(new FileReader(master_log));

        String line = br.readLine();
        String[] l;
        while (line != null) {
            l = line.split(" ");
            if (l[0].equals("Block")) {
                String[] tmp = new String[l.length-2];

                for (int i = 2; i < l.length; i++) {
                    usedFiles.add(l[i]);
                    tmp[i-2] = l[i];
                }
                needRecovery.put(Integer.parseInt(l[1]), tmp);
            } else {
                needRecovery.remove(Integer.parseInt(l[1]));
            }
        }
    }

	public static void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		UserNode UN = new UserNode(args[1]);
		PL = new ProjectLib( Integer.parseInt(args[0]), args[1], UN );
        queueMap = new HashMap<>();
        blockedTransaction = new HashSet<Integer>();
        usedFiles = new HashSet<String>();
        needRecovery = new HashMap<Integer, String[]>();

        File master_log = new File("master_log");
        if (master_log.exists()) recoveryRoutine();

        while (true) {
        }
	}
}

