package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */

    private final OpIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;
    private final TupleDesc td;
    private final TupleDesc childTd;
    private final boolean hasGroupBy;
    private final Aggregator aggregator;
    private Iterator<Tuple> it;

   public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.childTd = child.getTupleDesc();
        this.hasGroupBy = gfield != Aggregator.NO_GROUPING;
        this.aggregator = Aggregator.getAggregator(aop, childTd.getFieldType(afield), hasGroupBy);
        this.td = getTupleDesc();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
       return gfield;

    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
         return hasGroupBy ? childTd.getFieldName(gfield) : null;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
    
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return childTd.getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
       return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        child.open();
        it = child;
        aggregator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
          while (it.hasNext()) {
            Tuple t = it.next();
            aggregator.mergeTupleIntoGroup(t);
        }
        Tuple result = aggregator.iterator().next();
        aggregator.close();
        aggregator.reset();
        it = child;
        return result;
    }

    public void rewind() throws DbException, TransactionAbortedException {
         child.rewind();
        aggregator.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
         Type[] types = new Type[hasGroupBy ? 2 : 1];
        String[] names = new String[types.length];
        types[0] = hasGroupBy ? childTd.getFieldType(gfield) : childTd.getFieldType(afield);
        names[0] = hasGroupBy ? childTd.getFieldName(gfield) : childTd.getFieldName(afield);
        if (hasGroupBy) {
            types[1] = aggregator.getType();
            names[1] = aggregator.getName();
        }
        return new TupleDesc(types, names);
    }

    public void close() {
         child.close();
        aggregator.close();
    }

    @Override
    public OpIterator[] getChildren() {

         return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
       child = children[0];
    }

}
