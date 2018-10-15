package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;
	private final int gbFiled;
	private final Type gbFieldType;
	private final int afield;
	private final Op op;
	private final HashMap<Field, Integer> group;

	/**
	 * Aggregate constructor
	 *
	 * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
	 * @param afield      the 0-based index of the aggregate field in the tuple
	 * @param what        aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException if what != COUNT
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		this.gbFiled = gbfield;
		this.gbFieldType = gbfieldtype;
		this.afield = afield;
		if (what != Op.COUNT) {
			throw new IllegalArgumentException(String.format("the operator %s is not supported in StringAggregator", what));
		}
		this.op = what;
		this.group = new HashMap<>();
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the constructor
	 *
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		this.group.compute(tup.getField(gbFiled), (key, value) -> {
			if (value == null) {
				return 1;
			} else {
				return value + 1;
			}
		});
	}

	/**
	 * Create a OpIterator over group aggregate results.
	 *
	 * @return a OpIterator whose tuples are the pair (groupVal,
	 * aggregateVal) if using group, or a single (aggregateVal) if no
	 * grouping. The aggregateVal is determined by the type of
	 * aggregate specified in the constructor.
	 */
	public OpIterator iterator() {
		return new OpIterator() {
			private TupleDesc tupleDesc;
			private Iterator<Map.Entry<Field, Integer>> iter = null;

			@Override
			public void open() throws DbException, TransactionAbortedException {
				this.iter = group.entrySet().iterator();
			}

			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				return iter.hasNext();
			}

			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				final Map.Entry<Field, Integer> next = iter.next();
				final Integer value = next.getValue();
				final Tuple tuple = new Tuple(getTupleDesc());
				if(gbFiled == Aggregator.NO_GROUPING){
					tuple.setField(0, new IntField(value.intValue()));
				}else{
					tuple.setField(0, next.getKey());
					tuple.setField(1, new IntField(value.intValue()));
				}
				return tuple;
			}

			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				iter = group.entrySet().iterator();
			}

			@Override
			public TupleDesc getTupleDesc() {
				if (tupleDesc != null) {
					return tupleDesc;
				}
				if (gbFiled == Aggregator.NO_GROUPING) {
					this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
				} else {
					this.tupleDesc = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
				}
				return tupleDesc;
			}

			@Override
			public void close() {
				iter = null;
			}
		};
	}
}
