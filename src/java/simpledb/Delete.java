package simpledb;

import simpledb.buffer.BufferPoolManager;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private final OpIterator child;
    private final TransactionId tid;
    private final TupleDesc tupleDesc;
	private boolean executed = false;

	/**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
    	this.tid = t;
    	this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPoolManager#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(this.executed  == true){
    		return null;
	    }
	    int cnt = 0;
    	while(child.hasNext()){
		    final Tuple next = child.next();
		    try {
			    Database.getBufferPool().deleteTuple(tid, next);
		    } catch (IOException e) {
			    e.printStackTrace();
		    }
		    cnt += 1;
	    }
	    this.executed = true;
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
