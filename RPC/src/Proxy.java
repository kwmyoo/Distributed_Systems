/* Sample skeleton for proxy */

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.lang.SecurityManager;
import static java.lang.System.out;
import java.net.MalformedURLException;
import java.rmi.registry.*;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.Naming;
import java.rmi.RemoteException;

class Proxy {

    public static int MAXMSGLEN = 1024;
    public static HashMap<String, Integer> fdMap;
    public static HashMap<String, Integer> writeVersions;
    public static ArrayList<FileInterface> fileArray;
    private static ServerInterface server;
    public static Cache cache;
    public static String serverip;
    public static int port;
    public static long CACHE_LIMIT;
    public static String root;

    private static class FileHandler implements FileHandling {


        // copy file from local
        public void copyFile(String source, String dest) {
            InputStream is = null;
            OutputStream os = null;
            try {
                is = new FileInputStream(source);
                os = new FileOutputStream(dest);

                byte[] buffer = new byte[1024];
                int length;
                while ((length = is.read(buffer)) > 0) {
                    os.write(buffer, 0, length);
                }
                is.close();
                os.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        /*
         * @return: -1 when file is nonexistent on server, -2 when file is directory
         *          and -3 when encache failed
         */
        public synchronized int updateCache(String path, boolean readOnly) {
            FileInfo fi = new FileInfo(path);
            try{
                fi = server.checkFile(fi);
            } catch (RemoteException e) {
                return -1;
            }
            int serverVer = (int)fi.offset;
            long filelen = fi.length;
            if (serverVer == -1) return -1; // non existent

            String fullpath = root + path;
            String cachePath = String.format("%s-r%d", fullpath, serverVer);
            String oldCachePath = String.format("%s-r", fullpath);
            File newfile;
            boolean isDir;
            if (serverVer == -2) isDir = true;
            else isDir = false;
            boolean success;
            FileInterface target;
            // insert new file (for read) if not on cache
            if (!cache.containsPath(cachePath)) {
                cache.closeAllRead(oldCachePath);
                if ((filelen + cache.total_size) > CACHE_LIMIT) {
                    if (cache.evict(filelen) < 0) {
                        return -3;
                    }
                }
                success = download(path, cachePath);
                if (!success) return -3;

                target = new FileInterface(cachePath, filelen, true, isDir);
                if (!readOnly) target.isClosed = true;
                else target.readCount = 1;
                fileArray.add(target);
                fdMap.put(cachePath, fileArray.size() - 1);
                if (!cache.containsPath(cachePath)) cache.encache(target);
            } else { // if on cache, increment read count for read file
                if (readOnly) {
                    target = cache.findFileInterface(cachePath);
                    target.readCount++;
                }
            }
            String writePath;
            int writeVer;
            // if for writing, create new copy
            if (!readOnly) {
                if ((filelen + cache.total_size) > CACHE_LIMIT) {
                    if (cache.evict(filelen) < 0) {
                        return -3;
                    }
                }

                if (!writeVersions.containsKey(fullpath)) {
                    writeVersions.put(fullpath, 0);
                } else {
                    writeVersions.put(fullpath, writeVersions.get(fullpath) + 1);
                }
                writeVer = writeVersions.get(fullpath);
                writePath = String.format("%s-w%d", fullpath, writeVer);
                copyFile(cachePath, writePath);

                target = new FileInterface(writePath, filelen, readOnly, isDir);
                fileArray.add(target);
                fdMap.put(writePath, fileArray.size() - 1);
                cache.encache(target);
            }


            return serverVer;
        }

        /*
         * download the copy from server.
         * @return: true on success, false on failure
         */
        public synchronized boolean download(String origPath, String newPath) {
            FileInfo fi = new FileInfo(origPath);
            File newfile = new File(newPath);
            if (newfile.getParentFile() != null &&
                newfile.getParentFile().exists() == false) {
                new File(newfile.getParent()).mkdirs();
            }
            // create file to write in
            try {
                newfile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
            RandomAccessFile ranfile = null;
            try{
                ranfile = new RandomAccessFile(newfile, "rw");
            } catch (Exception e) {
                e.printStackTrace();
            }
            long len = 0;
            try{
                len = server.fileLength(fi);
            } catch (RemoteException e) {
                e.printStackTrace();
                return false;
            }
            int cnt = 0;
            int writelen;
            byte[] buf;

            // copy by chunks
            while (cnt < len) {
                if ((len - cnt) > MAXMSGLEN) writelen = MAXMSGLEN;
                else writelen = (int)len - cnt;

                try{
                    fi = new FileInfo(origPath, cnt, writelen);
                    buf = server.readFile(fi);
                    ranfile.seek(cnt);
                    ranfile.write(buf);
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }

                cnt += writelen;
            }

            try {
                ranfile.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            return true;
        }

		public synchronized int open( String path, OpenOption o ) {
            boolean readOnly;
            if (o == OpenOption.READ) readOnly = true;
            else readOnly = false;

            Cache.CacheNode cNode = cache.head;
            System.out.println("------------------");
            System.out.println(String.format("total size:%d", cache.total_size));
            while (cNode != null) {
                System.out.println(cNode.fi.path);
                cNode = cNode.next;
            }
            System.out.println("------------------");


            // check if the path is legal
            String fullpath = null;
            String dirpath = null;
            try {
                fullpath = new File(root + path).getCanonicalPath();
                dirpath = new File(root).getCanonicalPath();
                if (!fullpath.contains(dirpath)) return Errors.ENOENT;
            } catch (IOException e) {
                e.printStackTrace();
            }

            FileInfo fi;
            int status = updateCache(path, readOnly);
            if (status == -3) return Errors.ENOMEM;

            File newfile;
            String newPath;
            FileInterface fileInt;
            String dirPath = root + path;
			if ((o == OpenOption.READ) || (o == OpenOption.WRITE)) {
                if (status == -1) return Errors.ENOENT;
                if ((o == OpenOption.WRITE) && status == -2) return Errors.EISDIR;

            }
            else if (o == OpenOption.CREATE) {
                if (status == -1) {
                    fi = new FileInfo(path);
                    try {
                        if(!server.createFile(fi)) {
                            return Errors.EPERM;
                        }
                        writeVersions.put(dirPath, 0);
                        newPath = String.format("%s-w%d", dirPath, 0);
                        newfile = new File(newPath);
                        fileInt = new FileInterface(newPath, 0, false, false);
                        fileArray.add(fileInt);
                        fdMap.put(newPath, fileArray.size() - 1);
                        cache.encache(fileInt);
                    } catch (RemoteException e) {
                        return Errors.EBUSY;
                    }
                } else if (status == -2) {
                    return Errors.EISDIR;
                }
            }
            else if (o == OpenOption.CREATE_NEW) {
                if (status != -1) {
                    return Errors.EEXIST;
                } else {
                    try {
                        fi = new FileInfo(path);
                        if (!server.createFile(fi)) {
                            return Errors.EPERM;
                        }
                        writeVersions.put(dirPath, 0);
                        newPath = String.format("%s-w%d", dirPath, 0);
                        newfile = new File(newPath);
                        fileInt = new FileInterface(newPath, 0, false, false);
                        fileArray.add(fileInt);
                        fdMap.put(newPath, fileArray.size() - 1);
                        cache.encache(fileInt);
                    } catch (RemoteException e) {
                        return Errors.EBUSY;
                    }
                }
            }

            String conPath;
            if (status == -1) status = 0;
            if (readOnly) conPath = String.format("%s-r%d", dirPath, status);
            else conPath = String.format("%s-w%d", dirPath, writeVersions.get(dirPath));
            return fdMap.get(conPath);
        }


		public synchronized int close( int fd ) {
			if ((fd < 0) || (fd >= fileArray.size())) {
                return Errors.EBADF;
            }

            // update cache
            FileInfo fi;
            FileInterface newFileInt;
            long writelen;
            long cnt = 0;
            FileInterface target = fileArray.get(fd);
            long newSize = target.size;
            cache.moveToHead(target.path);
            target.readCount--;
            String origPath = target.path.split("-")[0];
            origPath = origPath.substring(root.length());
            String newPath;
            int newVer;
            File oldfile;

            // if the file is modified by write, update server and local files
            if (target.modified) {
                try{
                    while (cnt < newSize) {
                        if ((newSize - cnt) > MAXMSGLEN) {
                            writelen = MAXMSGLEN;
                        } else {
                            writelen = newSize - cnt;
                        }
                        fi = new FileInfo(origPath, cnt, writelen);
                        target.ranfile.seek(cnt);
                        target.ranfile.read(fi.buf);
                        server.writeFile(fi);
                        cnt += writelen;
                    }
                    fi = new FileInfo(origPath);
                    newVer = server.setVersionRemote(fi);
                    newPath = String.format("%s-r%d", root + origPath, newVer);
                    copyFile(target.path, newPath);
                    cache.decache(target.path);
                    cache.closeAllRead(String.format("%s-r", root + origPath));
                    newFileInt = new FileInterface(newPath, target.size, true, false);
                    newFileInt.isClosed = true;
                    fileArray.add(newFileInt);
                    fdMap.put(newPath, fileArray.size() - 1);
                    cache.encache(newFileInt);

                } catch (Exception e) {
                    return Errors.EBUSY;
                }
            }
            try{
                target.ranfile.seek(0);
            } catch (IOException e) {
                return Errors.EBUSY;
            }
            target.isClosed = true;
            return 0;
        }

		public synchronized long write( int fd, byte[] buf ) {
			if ((fd < 0) || (fd >= fileArray.size())) {
                return Errors.EBADF;
            }
            FileInterface target = fileArray.get(fd);
            if (target.readOnly) {
                return Errors.EBADF;
            }
            try{
                target.ranfile.write(buf);
            } catch (IOException e) {
                return Errors.EBUSY;
            }
            target.modified = true;
            if (target.size < buf.length) {
                cache.total_size -= target.size;
                cache.total_size += buf.length;
                target.size = buf.length;
            }
            return buf.length;
		}

		public synchronized long read( int fd, byte[] buf ) {
			if ((fd < 0) || (fd >= fileArray.size())) {
                return Errors.EBADF;
            }
            FileInterface target = fileArray.get(fd);
            if (target.isDir) {
                return Errors.EISDIR;
            }
            int result;
            try{
                result = target.ranfile.read(buf);
            } catch (IOException e) {
                e.printStackTrace();
                return Errors.EBUSY;
            }

            if (result == -1) {
                return 0;
            } else return result;
		}

		public synchronized long lseek( int fd, long pos, LseekOption o ) {
			if ((fd < 0) || (fd >= fileArray.size())) {
                return Errors.EBADF;
            }
            FileInterface target = fileArray.get(fd);
            long tmp;
            try {
                if (o == LseekOption.FROM_CURRENT) {
                    tmp = target.ranfile.getFilePointer();
                } else if (o == LseekOption.FROM_END) {
                    tmp = target.ranfile.length();
                } else if (o == LseekOption.FROM_START) {
                    tmp = 0;
                } else {
                    return Errors.EINVAL;
                }
            } catch(IOException e) {
                return Errors.EBUSY;
            }

            tmp += pos;
            if (tmp < 0) return Errors.EINVAL;
            try {
                target.ranfile.seek(tmp);
            } catch(IOException e) {
                return Errors.EBUSY;
            }
            return tmp;
		}

		public synchronized int unlink( String path ) {
			FileInfo fi = new FileInfo(path);
            try{
                fi = server.checkFile(fi);
            } catch (RemoteException e) {
                return Errors.EBUSY;
            }
            int status = (int)fi.offset;
            if (status == -2) {
                return Errors.EISDIR;
            }
            if (status == -1) {
                return Errors.ENOENT;
            }

            boolean success = false;
            try{
                success = server.unlinkFile(fi);
            } catch (RemoteException e) {
                return Errors.EBUSY;
            }
            if (success) return 0;
            else return Errors.EPERM;
		}

		public synchronized void clientdone() {
			return;
		}

	}

	private static class FileHandlingFactory implements FileHandlingMaking {
		public FileHandling newclient() {
			return new FileHandler();
		}
	}

	public static void main(String[] args) throws IOException {
        serverip = args[0];
        port = Integer.parseInt(args[1]);
        root = args[2];
        if (root.charAt(0) == '/') root = root.substring(1, root.length());
        if (root.charAt(root.length() - 1) != '/') root += '/';

        CACHE_LIMIT = (long)Integer.parseInt(args[3]);
        String socket;
        fdMap = new HashMap<String, Integer>();
        fileArray = new ArrayList<FileInterface>();
        writeVersions = new HashMap<String, Integer>();
        cache = new Cache();

        try{
            socket = String.format("//%s:%d/ServerService", serverip, port);
            server = (ServerInterface) Naming.lookup(socket);
        } catch (Exception e) {
            System.err.println("error");
        }
		(new RPCreceiver(new FileHandlingFactory())).run();
	}
}

