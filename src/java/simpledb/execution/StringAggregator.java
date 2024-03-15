package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.HashMap;
import simpledb.storage.Iterator;
import simpledb.storage.TupleIterator;
import simpledb.common.Utility;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc td;
    private HashMap<Field, Integer> groupMap;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (gbfield == Aggregator.NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
        groupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
     public void mergeTupleIntoGroup(Tuple tup) {
        Field groupVal = (gbfield == Aggregator.NO_GROUPING) ? null : tup.getField(gbfield);
        Field aggregateVal = tup.getField(afield);
        if (!(aggregateVal instanceof StringField)) {
            throw new IllegalArgumentException("StringAggregator only supports aggregation over StringFields");
        }
       if (!groupMap.containsKey(groupVal)) {
            groupMap.put(groupVal, 1);
        } else {
            groupMap.put(groupVal, groupMap.get(groupVal) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */


        public OpIterator iterator() {
        return new TupleIterator() {
            private Iterator<Map.Entry<Field, Integer>> it;

            {
                it = groupMap.entrySet().iterator();
            }

            public void open() throws DbException, NoSuchElementException {
                if (groupMap.isEmpty()) {
                    throw new NoSuchElementException("No groups to iterate over");
                }
            }

            public TupleDesc getTupleDesc() {
                return td;
            }

            public boolean hasNext() throws DbException {
                return it.hasNext();
            }

            public Tuple next() throws DbException, NoSuchElementException {
                Map.Entry<Field, Integer> entry = it.next();
                Field groupVal = entry.getKey();
                int count = entry.getValue();
                Tuple tuple = new Tuple(td);
                if (gbfield == Aggregator.NO_GROUPING) {
                    tuple.setField(0, new IntField(count));
                } else {
                    tuple.setField(0, groupVal);
                    tuple.setField(1, new IntField(count));
                }
                return tuple;
            }

            public void rewind() throws DbException, NoSuchElementException {
                it = groupMap.entrySet().iterator();
            }

            public void close() {
                groupMap = null;
            }
        };
    }


}
