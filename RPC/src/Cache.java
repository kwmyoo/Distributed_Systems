import java.io.*;

public class Cache {

    public class CacheNode {
        public FileInterface fi;
        public CacheNode next;
        public CacheNode prev;

        public CacheNode(FileInterface fi) {
            this.fi = fi;
            this.next = null;
            this.prev = null;
        }
    }

    public long total_size;
    public CacheNode head;
    public CacheNode tail;


    public Cache() {
        total_size = 0;
        head = null;
        tail = null;
    }

    // add new element to cache
    public synchronized void encache(FileInterface fi) {
        CacheNode newNode = new CacheNode(fi);
        if ((head == null) || (tail == null)) {
            head = newNode;
            tail = newNode;
        }
        else {
            head.prev = newNode;
            newNode.next = head;
            head = newNode;
        }
        total_size += fi.size;
    }

    // remove specific element from cache
    // @return: true on success, false on failure
    public synchronized boolean decache(String pathname) {
        CacheNode target = head;
        while (target != null) {
            if (pathname.equals(target.fi.path)) {
                break;
            }
            target = target.next;
        }
        if (target.fi.readOnly && (target.fi.readCount > 0)) return true;
        if ((!target.fi.readOnly) && (!target.fi.isClosed)) return true;
        try {
            File targetfile = new File(target.fi.path);
            targetfile.delete();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        // if target is the only element
        if (head == tail) {
            head = null;
            tail = null;
            total_size = 0;
        } else if (target == head) { // if head
            target.next.prev = null;
            head =  target.next;
            total_size -= target.fi.size;
        } else if (target == tail) { // if tail
            target.prev.next = null;
            tail = target.prev;
            total_size -= target.fi.size;
        } else {
            target.next.prev = target.prev;
            target.prev.next = target.next;
            total_size -= target.fi.size;
        }
        return true;
    }


    // evict by LRU policy
    public synchronized int evict(long newsize) {
        if ((head == null) || (tail == null)) return 0;

        CacheNode target = tail;
        while ((target.fi.size < newsize) || !(target.fi.isClosed)) {
            target = target.prev;
            if (target == null) break;
        }

        // when nothing can be evicted, or deleting file fails
        if (target == null) return -1;
        boolean success = false;
        try {
            File targetfile = new File(target.fi.path);
            success = targetfile.delete();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (!success) return -1;

        // if target is the only element
        if (head == tail) {
            head = null;
            tail = null;
            total_size = 0;
        } else if (target == head) { // if head
            target.next.prev = null;
            head =  target.next;
            total_size -= target.fi.size;
        } else if (target == tail) { // if tail
            target.prev.next = null;
            tail = target.prev;
            total_size -= target.fi.size;
        } else {
            target.next.prev = target.prev;
            target.prev.next = target.next;
            total_size -= target.fi.size;
        }
        return 0;
    }

    // check if the path is cached
    public boolean containsPath(String pathname) {
        CacheNode targetNode = head;

        while (targetNode != null) {
            if (pathname.equals(targetNode.fi.path)) {
                return true;
            }
            targetNode = targetNode.next;
        }
        return false;
    }

    // close all outdated read files
    public void closeAllRead(String pathname) {
        CacheNode targetNode = head;
        CacheNode tmp;

        while (targetNode != null) {
            if (targetNode.fi.path.contains(pathname)) {
                tmp = targetNode.next;
                decache(targetNode.fi.path);
                targetNode = tmp;
            }
            else {
                targetNode = targetNode.next;
            }
        }
    }

    // look for particular FileInterface with pathname
    public FileInterface findFileInterface(String pathname) {
        CacheNode targetNode = head;
        CacheNode tmp;

        while (targetNode != null) {
            if (pathname.equals(targetNode.fi.path)) {
                return targetNode.fi;
            }
            targetNode = targetNode.next;
        }
        return null;
    }

    // move the cache node with pathname to the front of the cache
    public void moveToHead(String pathname) {
        CacheNode targetNode = head;

        while ((targetNode != null) && !pathname.equals(targetNode.fi.path)) {
            targetNode = targetNode.next;
        }

        // if target is the only element
        if (head == tail) {
            // do nothing
        } else if (targetNode == head) { // if head
            // do nothing
        } else if (targetNode == tail) { // if tail
            targetNode.prev.next = null;
            tail = targetNode.prev;
            head.prev = targetNode;
            targetNode.next = head;
            head = targetNode;
        } else {
            targetNode.next.prev = targetNode.prev;
            targetNode.prev.next = targetNode.next;
            head.prev = targetNode;
            targetNode.next = head;
            head = targetNode;
        }
    }

}


