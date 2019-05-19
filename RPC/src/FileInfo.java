import java.io.*;

public class FileInfo implements java.io.Serializable{
    public String path;
    public byte[] buf;
    public long offset;
    public long length;

    public FileInfo(String pathname) {
        path = pathname;
    }

    public FileInfo(String pathname, long oft, long len) {
        path = pathname;
        buf = new byte[(int)len];
        length = len;
        offset = oft;
    }
}
