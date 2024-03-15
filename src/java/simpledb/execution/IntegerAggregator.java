package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;

import java.util.HashMap;
import java.util.Map;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> groupCounts;
    private Map<Field, Integer> groupSums;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupCounts = new HashMap<>();
        this.groupSums = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
     @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupVal = gbfieldtype == null ? null : tup.getField(gbfield);
        int aggVal = tup.getField(afield).getInt();
        if (!groupCounts.containsKey(groupVal)) {
            groupCounts.put(groupVal, 0);
            groupSums.put(groupVal, 0);
        }
        groupCounts.put(groupVal, groupCounts.get(groupVal) + 1);
        groupSums.put(groupVal, groupSums.get(groupVal) + aggVal);
    }
    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    
    @Override
    public OpIterator iterator() {
        return new IntegerAggregatorOpIterator(groupCounts, groupSums, what, gbfieldtype, afield);
    }



    // Implementation of the methos OPIterator  

    private static class IntegerAggregatorOpIterator implements OpIterator {

        private Map<Field, Integer> groupCounts;
        private Map<Field, Integer> groupSums;
        private Op what;
        private Type gbfieldtype;
        private int afield;
        private Iterator<Map.Entry<Field, Integer>> iterator;

        public IntegerAggregatorOpIterator(Map<Field, Integer> groupCounts, Map<Field, Integer> groupSums, Op what, Type gbfieldtype, int afield) {
            this.groupCounts = groupCounts;
            this.groupSums = groupSums;
            this.what = what;
            this.gbfieldtype = gbfieldtype;
            this.afield = afield;
            this.iterator = groupCounts.entrySet().iterator();
        }

        @Override
        public void open() throws DbException, NoSuchElementException {
            if (iterator.hasNext()) {
                iterator.next();
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public boolean hasNext() throws DbException {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, NoSuchElementException {
            Map.Entry<Field, Integer> entry = iterator.next();
            Field groupVal = entry.getKey();
            int count = entry.getValue();
            int sum = groupSums.get(groupVal);
            TupleDesc tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggVal"});
            Tuple tuple = new Tuple(tupleDesc);
            if (gbfieldtype != null) {
                tuple.setField(0, groupVal);
            }
            switch (what) {
                case COUNT:
                    tuple.setField(1, new IntField(count));
                    break;
                case SUM:
                    tuple.setField(1, new IntField(sum));
                    break;
                case AVG:
                    tuple.setField(1, new IntField(sum / count));
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + what);
            }
            return tuple;
        }

        @Override
        public void rewind() throws DbException, NoSuchElementException {
            iterator = groupCounts.entrySet().iterator();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggVal"});
        }

        @Override
        public void close() {
            // do nothing
        }
    }

}
