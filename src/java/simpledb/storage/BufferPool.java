package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    
    private int numPages;
    private ConcurrentHashMap<PageId, Page> pages;
    private ConcurrentHashMap<PageId, Page> dirtiedPages;
    private ConcurrentHashMap<PageId, ConcurrentHashMap<Permissions, LinkedList<TransactionId>>> locks;
    private ConcurrentHashMap<PageId, LinkedList<TransactionId>> waitingTransactions = new ConcurrentHashMap<PageId, LinkedList<TransactionId>>();
    private final Lock lock = new ReentrantLock();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.pageSize = DEFAULT_PAGE_SIZE;
        this.numPages = numPages;
        this.pages = new ConcurrentHashMap<PageId, Page>();
        this.locks = new ConcurrentHashMap<PageId, ConcurrentHashMap<Permissions, LinkedList<TransactionId>>>();
        this.dirtiedPages = new ConcurrentHashMap<PageId, Page>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
           Page page = pages.get(pid);
        if (page != null) {
            if (perm.compareTo(page.getPermissions()) < 0) {
                throw new DbException("Permission denied: " + pid + ", " + perm);
            }
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */


    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
         Page page = pages.get(pid);
        if (page != null) {
        page.releaseLock(tid);
         }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
       for (Map.Entry<PageId, LinkedList<TransactionId>> entry : waitingTransactions.entrySet()) {
            LinkedList<TransactionId> waitingTransactions = entry.getValue();
            waitingTransactions.remove(tid);
            if (waitingTransactions.isEmpty()) {
                lock.release();
            }
        }
        waitingTransactions.clear();
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
          ConcurrentHashMap<Permissions, LinkedList<TransactionId>> lockMap = locks.get(p);
        if (lockMap == null) {
            return false;
        }

        for (Map.Entry<Permissions, LinkedList<TransactionId>> entry : lockMap.entrySet()) {
            LinkedList<TransactionId> waitingTransactions = entry.getValue();
            if (waitingTransactions.contains(tid)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if (commit) {
            for (Map.Entry<PageId, Page> entry : dirtiedPages.entrySet()) {
                Page page = entry.getValue();
                try {
                    flushPage(page.getId());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            for (Map.Entry<PageId, Page> entry : dirtiedPages.entrySet()) {
                Page page = entry.getValue();
                try {
                    discardPage(page.getId());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        transactionComplete(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
       HeapPageId pid = (HeapPageId) t.getRecordId().getPageId();
        Page page = pages.get(pid);
        if (page == null) {
            page = Database.getCatalog().getDatabaseFile(tableId).readPage(pid);
            pages.put(pid, page);
        }

        synchronized (page) {
            page.acquireWriteLock(tid);
            page.markDirty(true, tid);
            page.insertTuple(t);
        }

        dirtiedPages.put(pid, page);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        HeapPageId pid = (HeapPageId) t.getRecordId().getPageId();
        Page page = pages.get(pid);
        if (page == null) {
            page = Database.getCatalog().getDatabaseFile(t.getRecordId().getTableId()).readPage(pid);
           pages.put(pid, page);
        }

        synchronized (page) {
            page.acquireWriteLock(tid);
            page.markDirty(true, tid);
            page.deleteTuple(t);
        }

        dirtiedPages.put(pid, page);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page page : dirtiedPages.values()) {
            flushPage(page.getId());
        }
        dirtiedPages.clear();

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
         Page page = pages.remove(pid);
        if (page != null) {
            dirtiedPages.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
         Page page = pages.get(pid);
        if (page == null) {
            return;
        }

        if (!page.isDirty()) {
            return;
        }

        Database.getLog().logWrite(page);
        Database.getDiskManager().writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
       for (Page page : dirtiedPages.values()) {
            if (page.isDirty() && page.getTransactionId().equals(tid)) {
                flushPage(page.getId());
                }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
       if (pages.isEmpty()) {
            return;
        }

        Page page = null;
        PageId pid = null;
        long minLRU = Long.MAX_VALUE;
        for (Map.Entry<PageId, Page> entry : pages.entrySet()) {
            Page p = entry.getValue();
            if (!p.isDirty() && p.getLRU() < minLRU) {
                minLRU = p.getLRU();
                page = p;
                pid = p.getId();
            }
        }

        if (page != null) {
            pages.remove(pid);
            dirtiedPages.remove(pid);
            try {
                flushPage(pid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    
    }
    
}
