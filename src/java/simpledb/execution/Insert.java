package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.BufferPool;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
        private final TransactionId t;
    private final OpIterator child;
    private final int tableId;
    private final TupleDesc tupleDesc;
    private final BufferPool bufferPool;
    private boolean opened;


    public Insert(TransactionId t, OpIterator child, int tableId)throws DbException {
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = child.getTupleDesc();
        this.bufferPool = Database.getBufferPool();
        this.opened = false;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return TupleDesc.getTupleDesc(new Type[]{Type.INT_TYPE});
    }

   @Override
    public void open() throws DbException, TransactionAbortedException {
        if (opened) {
            throw new IllegalStateException("Insert operator is already open.");
        }
        child.open();
        opened = true;
    }

   
    @Override
    public void close() {
        child.close();
        opened = false;
    }

    
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!opened) {
            throw new IllegalStateException("Insert operator is not open.");
        }

        int count = 0;
        while (child.hasNext()) {
            Tuple next = child.next();
            try {
                bufferPool.insertTuple(t, tableId, next);
                count++;
            } catch (BufferPool.PageNotReadException | BufferPool.InvalidPageException | BufferPool.PageUnpinnedException e) {
                throw new DbException("Failed to insert tuple.", e);
            }
        }

        if (count > 0) {
            return new Tuple(getTupleDesc());
        } else {
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};

    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length != 1) {
            throw new IllegalArgumentException("Insert operator expects exactly one child.");
        }
        child = children[0];;
    }

}
