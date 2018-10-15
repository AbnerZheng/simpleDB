package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;
	private final int gbField;
	private final Type gbFieldType;
	private final int afield;
	private final Op op;
	private final HashMap<Field, IntegerAggregateInfo> group;

	/**
	 * Aggregate constructor
	 *
	 * @param gbfield     the 0-based index of the group-by field in the tuple, or
	 *                    NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
	 *                    if there is no grouping
	 * @param afield      the 0-based index of the aggregate field in the tuple
	 * @param what        the aggregation operator
	 */

	public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		this.gbField = gbfield;
		this.gbFieldType = gbfieldtype;
		this.afield = afield;
		this.op = what;
		this.group = new HashMap<>();
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 *
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		final Field field = tup.getField(afield);
		assert field.getType() == Type.INT_TYPE;
		final IntField field1 = (IntField) field;
		final Field gbField = tup.getField(this.gbField);
		this.group.compute(gbField, (k, v) -> {
			if (v == null) {
				v = new IntegerAggregateInfo();

			}
			v.addField(field1.getValue());
			return v;
		});

	}

	/**
	 * Create a OpIterator over group aggregate results.
	 *
	 * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
	 * if using group, or a single (aggregateVal) if no grouping. The
	 * aggregateVal is determined by the type of aggregate specified in
	 * the constructor.
	 */
	public OpIterator iterator() {
		return new OpIterator() {

			private TupleDesc tupleDesc;
			private Iterator<Map.Entry<Field, IntegerAggregateInfo>> iter;

			@Override
			public void open() throws DbException, TransactionAbortedException {
				this.iter = group.entrySet().iterator();
			}

			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				return this.iter.hasNext();
			}

			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {

				if (iter.hasNext()) {
					final Map.Entry<Field, IntegerAggregateInfo> next = iter.next();
					final IntegerAggregateInfo aggregateInfo = next.getValue();

					int value;
					switch (op) {
						case MIN:
							value = aggregateInfo.min;
							break;
						case MAX:
							value = aggregateInfo.max;
							break;
						case COUNT:
							value = aggregateInfo.cnt;
							break;
						case SUM:
							value = aggregateInfo.sum;
							break;
						default:
							value = (int) (aggregateInfo.sum / aggregateInfo.cnt);
							break;
					}
					Tuple tuple = new Tuple(getTupleDesc());
					if (gbField == Aggregator.NO_GROUPING) {
						tuple.setField(0, new IntField(value));
					} else {
						tuple.setField(0, next.getKey());
						tuple.setField(1, new IntField(value));
					}
					return tuple;
				}
				throw new NoSuchElementException();
			}

			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				this.iter = group.entrySet().iterator();
			}

			@Override
			public TupleDesc getTupleDesc() {
				if (tupleDesc != null) {
					return tupleDesc;
				}
				if (gbField == Aggregator.NO_GROUPING) {
					this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
				} else {
					this.tupleDesc = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
				}
				return tupleDesc;
			}

			@Override
			public void close() {
				this.iter = null;
			}
		};
	}

	private class IntegerAggregateInfo {
		public int min = Integer.MAX_VALUE;
		public int max = Integer.MIN_VALUE;
		public int sum = 0;
		public int cnt = 0;

		public void addField(int f) {
			if (f > max) max = f;
			if (f < min) min = f;
			cnt++;
			sum += f;
		}
	}
}
