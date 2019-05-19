/*
 * This class defines the macros for the types of messages
 */

public class MsgMacros {
    // message types between server and clients
    // from server to clients
    public static byte VOTE_REQUEST = 0;
    public static byte GLOBAL_COMMIT = 1;
    public static byte ABORT = 2;

    // from client to server
    public static byte VOTE_YES = 3;
    public static byte VOTE_NO = 4;
    public static byte ACK = 5;

    // index of each elements in message.body
    public static int ID = 0;
    public static int MSG_TYPE = 1;
    // index of number of source files for each client
    public static int NUM_FILES = 2;
}

