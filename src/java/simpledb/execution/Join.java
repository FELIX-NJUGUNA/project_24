import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    // The predicate to use to join the children
    private JoinPredicate joinPredicate;

    // Iterator for the left(outer) relation to join
    private OpIterator child1;

    // Iterator for the right(inner) relation to join
    private OpIterator child2;

    // The tuple descriptor of the joined tuples
    private TupleDesc tupleDesc;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;
        this.tupleDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    /**
     * @return The predicate used to join the children
     */
    public JoinPredicate getJoinPredicate() {
        return joinPredicate;
    }

    /**
     * @return The field name of join field1
     */
    public String getJoinField1Name() {
        return child1.getTupleDesc().getFieldName(joinPredicate.getField1());
    }

    /**
     * @return The field name of join field2
     */
    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(joinPredicate.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
        child1.open();
        child2.open();
        super.open();
    }

    public void close() {
        child1.close();
        child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        Tuple t1 = null;
        Tuple t2 = null;

        while (t1 == null || t2 == null) {
            if (t1 == null) {
                t1 = child1.next();
            }
            if (t2 == null) {
                while (t1 != null && !joinPredicate.filter(t1, t2)) {
                    t2 = child2.next();
                    if (t2 == null) {
                        t1 = child1.next();
                    }
                }
            }
        }

        Tuple joinedTuple = new Tuple(tupleDesc);
        int i = 0;
        for (; i < child1.getTupleDesc().numFields(); i++) {
            joinedTuple.setField(i, t1.getField(i));
        }
        for (int j = 0; i < tupleDesc.numFields(); i++, j++) {
            joinedTuple.setField(i, t2.getField(j));
        }

        return joinedTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child1, child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length != 2) {
            throw new IllegalArgumentException("Join expects 2 children");
        }
        this.child1 = children[0];
        this.child2 = children[1];
    }
}