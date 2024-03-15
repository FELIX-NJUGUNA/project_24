package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

     private static final String TAG = "HeapFile";
    private File file;
    private TupleDesc tupleDesc;
    private int id;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
        this.id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
          return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        
        throw new UnsupportedOperationException("implement this");
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        throw new UnsupportedOperationException("implement this");
         return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
         try {
            RandomAccessFile file = new RandomAccessFile(this.file, "r");
            file.seek(pid.getPageNumber() * BufferPool.getPageSize());
            byte[] pageData = new byte[BufferPool.getPageSize()];
            file.readFully(pageData);
            file.close();
            return new HeapPage(pid, pageData);
        } catch (IOException e) {
            e.printStackTrace();
            throw new DbException(TAG + ": Error reading page " + pid);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil((double) file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
         List<Page> pages = new ArrayList<>();
        int pageNum = 0;
        boolean inserted = false;
        while (!inserted) {
            HeapPageId pid = new HeapPageId(getId(), pageNum);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
              page.insertTuple(t);
                Database.getBufferPool().unpinPage(tid, pid, true);
                inserted = true;
            } else {
                Database.getBufferPool().unpinPage(tid, pid, false);
                pageNum++;
            }
        }
        pages.add(new HeapPage(new HeapPageId(getId(), pageNum - 1), page.getPageData()));
        return pages;

    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> pages = new ArrayList<>();
        int pageNum = (int) t.getRecordId().getPageId().getPageNumber();
        HeapPageId pid = new HeapPageId(getId(), pageNum);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        Database.getBufferPool().unpinPage(tid, pid, true);
        pages.add(new HeapPage(new HeapPageId(getId(), pageNum), page.getPageData()));
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
      return new HeapFileIterator(tid, this);
    }

}

