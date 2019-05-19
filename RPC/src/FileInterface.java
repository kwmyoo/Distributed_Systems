import java.io.*;

class FileInterface {
    public String path;
    public long size;
    public boolean readOnly;
    public boolean isDir;
    public boolean modified;
    public boolean isClosed;
    public RandomAccessFile ranfile;
    public int readCount;

    public FileInterface(String path, long size, boolean readOnly,
            boolean isDir) {
        this.path = path;
        this.size = size;
        this.readOnly = readOnly;
        this.isDir = isDir;
        this.modified = false;
        this.isClosed = false;
        this.readCount = 0;

        try {
            this.ranfile = new RandomAccessFile(path, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
