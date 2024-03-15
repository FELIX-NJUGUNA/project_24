package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private final TransactionId tid;
    private final OpIterator child;
    private final int tableId;
    private final TupleDesc tupleDesc;
    private Tuple next;
    private boolean opened;
    private int deletedCount;

      public Delete(TransactionId t, OpIterator child, int tableId) {
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
         child.open();
        opened = true;
    }

    public void close() {
       child.close();
        opened = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        deletedCount = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!opened) {
            throw new IllegalStateException("Operator is not open.");
        }

        while (true) {
            if (next != null) {
                Tuple result = next;
                next = null;
                return result;
            }

            Tuple tuple = child.next();
            if (tuple == null) {
                break;
            }

            try {
                Database.getBufferPool().deleteTuple(tid, tableId, tuple);
                deletedCount++;
            } catch (IOException e) {
                throw new DbException("Failed to delete tuple.", e);
            }
        }

        Tuple result = new Tuple(tupleDesc);
        result.setField(0, new IntField(deletedCount));
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
         if (children.length != 1) {
            throw new IllegalArgumentException("Delete operator expects exactly one child.");
        }
        child = children[0];
    }

}
