import simpledb.common.Type;
import simpledb.common.Utility;
import simpledb.execution.Predicate;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Field;

/**
 * Filter operator. Filters tuples based on a given predicate.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    // The predicate used to filter tuples
    private Predicate p;

    // The child operator that provides tuples to filter
    private OpIterator child;

    // The tuple descriptor of the child operator
    private TupleDesc td;

    /**
     * Constructor.
     *
     * @param p
     *            The predicate used to filter tuples
     * @param child
     *            The child operator that provides tuples to filter
     */
    public Filter(Predicate p, OpIterator child) {
        this.p = p;
        this.child = child;
        this.td = child.getTupleDesc();
    }

    /**
     * Returns the predicate used to filter tuples.
     *
     * @return The predicate used to filter tuples
     */
    public Predicate getPredicate() {
        return p;
    }

    /**
     * Returns the tuple descriptor of the child operator.
     *
     * @return The tuple descriptor of the child operator
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     * Opens the filter operator.
     *
     * @throws DbException
     *             if the child operator cannot be opened
     * @throws NoSuchElementException
     *             if the child operator has no more tuples
     * @throws TransactionAbortedException
     *             if the transaction is aborted
     */
    public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
child.open();
        super.open();
    }

    /**
     * Closes the filter operator.
     */
    public void close() {
        child.close();
        super.close();
    }

    /**
     * Rewinds the child operator.
     *
     * @throws DbException
     *             if the child operator cannot be rewound
     * @throws TransactionAbortedException
     *             if the transaction is aborted
     */
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Returns the next tuple that passes the filter.
     *
     * @return The next tuple that passes the filter, or null if there are no more
     *         tuples
     * @throws NoSuchElementException
     *             if the child operator has no more tuples
     * @throws TransactionAbortedException
     *             if the transaction is aborted
     * @throws DbException
     *             if the child operator cannot be opened
     */
    protected Tuple fetchNext() throws NoSuchElementException, TransactionAbortedException, DbException {
        while (true) {
            Tuple next = child.next();
            if (next == null) {
                return null;
            }
            if (p.filter(next)) {
                return next;
            }
        }
    }

    /**
     * Returns the children of this operator.
     *
     * @return The children of this operator
     */
    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    /**
     * Sets the children of this operator.
     *
     * @param children
     *            The children of this operator
     */
    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length != 1) {
            throw new IllegalArgumentException("Filter operator only has one child");
        }
        this.child = children[0];
    }
}