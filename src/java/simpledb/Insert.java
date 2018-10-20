package simpledb;

import simpledb.buffer.BufferPoolManager;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private final OpIterator child;
    private final int tableId;
    private final TupleDesc tupleDesc;
	private boolean executed = false;

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
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
    	this.tid = t;
    	this.child = child;
    	this.tableId = tableId;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
    	return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
    	child.open();
    }

    public void close() {
    	super.close();
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPoolManager. An
     * instances of BufferPoolManager is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPoolManager#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(this.executed == true){
			return null;
	    }
    	int cnt = 0;
    	while(child.hasNext()){
		    final Tuple next = child.next();
		    try {
			    Database.getBufferPool().insertTuple(tid, tableId, next);
		    }catch (IOException e){

		    }
		    cnt += 1;
	    }
	    this.executed =true;
	    final Tuple tuple = new Tuple(this.tupleDesc);
    	tuple.setField(0, new IntField(cnt));
    	return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }
}
