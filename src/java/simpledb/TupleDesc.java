package simpledb;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	private final TDItem[] fields;
	private int size;

	/**
	 * A help class to facilitate organizing the information of each field
	 */
	public static class TDItem implements Serializable {

		private static final long serialVersionUID = 1L;

		/**
		 * The type of the field
		 */
		public final Type fieldType;

		/**
		 * The name of the field
		 */
		public final String fieldName;

		public TDItem(Type t, String n) {
			this.fieldName = n;
			this.fieldType = t;
		}

		public String toString() {
			return fieldName + "(" + fieldType + ")";
		}
	}

	/**
	 * @return An iterator which iterates over all the field TDItems
	 * that are included in this TupleDesc
	 */
	public Iterator<TDItem> iterator() {
		// some code goes here
		return null;
	}

	private static final long serialVersionUID = 1L;

	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the
	 * specified types, with associated named fields.
	 *
	 * @param typeAr  array specifying the number of and types of fields in this
	 *                TupleDesc. It must contain at least one entry.
	 * @param fieldAr array specifying the names of the fields. Note that names may
	 *                be null.
	 */
	public TupleDesc(Type[] typeAr, String[] fieldAr) {
		assert typeAr.length == fieldAr.length;

		this.fields = new TDItem[typeAr.length];
		for (int i = 0; i < typeAr.length; i++) {
			TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
			this.fields[i] = tdItem;
		}
		calculateSize();
	}

	/**
	 * Constructor. Create a new tuple desc with typeAr.length fields with
	 * fields of the specified types, with anonymous (unnamed) fields.
	 *
	 * @param typeAr array specifying the number of and types of fields in this
	 *               TupleDesc. It must contain at least one entry.
	 */
	public TupleDesc(Type[] typeAr) {
		this.fields = new TDItem[typeAr.length];
		for (int i = 0; i < typeAr.length; i++) {
			TDItem tdItem = new TDItem(typeAr[i], null);
			this.fields[i] = tdItem;
		}
		calculateSize();
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		// some code goes here
		return this.fields.length;
	}

	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 *
	 * @param i index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException {
		return this.fields[i].fieldName;
	}

	/**
	 * Gets the type of the ith field of this TupleDesc.
	 *
	 * @param i The index of the field to get the type of. It must be a valid
	 *          index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public Type getFieldType(int i) throws NoSuchElementException {
		if (i < 0 || i >= numFields()) {
			throw new NoSuchElementException();
		}
		return this.fields[i].fieldType;
	}

	/**
	 * Find the index of the field with a given name.
	 *
	 * @param name name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException if no field with a matching name is found.
	 */
	public int fieldNameToIndex(String name) throws NoSuchElementException {
		if (name == null) {
			throw new NoSuchElementException();
		}
		for (int i = 0; i < numFields(); i++) {
			if (name.equals(fields[i].fieldName)) {
				return i;
			}
		}
		throw new NoSuchElementException();
	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc.
	 * Note that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		return this.size;
	}

	private void calculateSize() {
		int s = 0;
		for (TDItem tdItem : this.fields) {
			s += tdItem.fieldType.getLen();
		}
		this.size = s;
	}

	/**
	 * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
	 * with the first td1.numFields coming from td1 and the remaining from td2.
	 *
	 * @param td1 The TupleDesc with the first fields of the new TupleDesc
	 * @param td2 The TupleDesc with the last fields of the TupleDesc
	 * @return the new TupleDesc
	 */
	public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
		int s = td1.numFields() + td2.numFields();
		Type[] types = new Type[s];
		String[] fields = new String[s];
		for (int i = 0; i < td1.numFields(); i++) {
			types[i] = td1.getFieldType(i);
			fields[i] = td1.getFieldName(i);
		}
		for (int i = 0; i < td2.numFields(); i++) {
			types[i + td1.numFields()] = td2.getFieldType(i);
			fields[i + td1.numFields()] = td2.getFieldName(i);
		}
		return new TupleDesc(types, fields);
	}

	/**
	 * Compares the specified object with this TupleDesc for equality. Two
	 * TupleDescs are considered equal if they have the same number of items
	 * and if the i-th type in this TupleDesc is equal to the i-th type in o
	 * for every i.
	 *
	 * @param o the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */

	public boolean equals(Object o) {
		if (o instanceof TupleDesc) {
			TupleDesc o1 = (TupleDesc) o;
			if (o1.numFields() == numFields()) {
				for (int i = 0; i < o1.numFields(); i++) {
					if (!getFieldType(i).equals(o1.getFieldType(i))) {
						return false;
					}
				}
				return true;
			}
			return false;
		}
		return false;
	}

	public int hashCode() {
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
		throw new UnsupportedOperationException("unimplemented");
	}

	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
	 * the exact format does not matter.
	 *
	 * @return String describing this descriptor.
	 */
	public String toString() {
		String s = "";
		for (int i = 0; i < numFields(); i++) {
			s += getFieldType(i).toString() + "(" + getFieldName(i) + ")";
			if (i != numFields()) s += ",";
		}
		return s;
	}
}
