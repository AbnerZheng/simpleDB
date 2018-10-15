package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import static java.io.File.separator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Field[] fields;
    private TupleDesc tupleDesc;
    private RecordId rid;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
    	this.tupleDesc = td;
    	this.fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
    	return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
    	this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
    	fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
    	return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
    	StringBuilder s = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            s.append(fields[i].toString());
            if(i != fields.length - 1){
                s.append("\t");
            }
        }
        return s.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
    	return Arrays.asList(fields).iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
    	this.tupleDesc = td;
    }

    @Override
    public boolean equals(final Object obj) {
    	if(this == obj){
    	    return true;
        }
        if(obj instanceof Tuple){
            final Tuple obj1 = (Tuple) obj;
            if(obj1.getTupleDesc().equals(this.getTupleDesc())){
                if(obj1.getRecordId().equals(getRecordId())){
                    if(obj1.fields.length == fields.length){
                        for (int i = 0; i < obj1.fields.length; i++) {
                            if(!obj1.getField(i).equals(getField(i))){
                                return false;
                            }
                        }
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
