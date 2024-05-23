package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    /**
     * a list that stores all the items
     */
    CopyOnWriteArrayList<TDItem> tdItems;




    /**
     * public method to get the list
     */
    public CopyOnWriteArrayList<TDItem> getTdItems() {
        return tdItems;
    }

    // override the clone function for later use
    @Override
    public TupleDesc clone() {
        try {
            return (TupleDesc) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
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
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        if(tdItems == null){
            return null;
        }
        return tdItems.iterator();

    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        tdItems = new CopyOnWriteArrayList<>();
        for(int i = 0; i < typeAr.length; ++i){
            tdItems.add(new TDItem(typeAr[i],fieldAr[i]));
        }

    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        tdItems = new CopyOnWriteArrayList<>();
        for (Type type : typeAr) {
            tdItems.add(new TDItem(type, null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public TupleDesc(){
        tdItems = new CopyOnWriteArrayList<>();
    }

    public int numFields() {
        // some code goes here
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i >= tdItems.size()){
            throw new NoSuchElementException("i is not a valid field reference.");
        }
        return tdItems.get(i).fieldName;

    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i >= tdItems.size()){
            throw new NoSuchElementException("i is not a valid field reference.");
        }
        return tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null){
            throw new NoSuchElementException("no field with a matching name is found");
        }

        for(int i = 0;i < tdItems.size(); ++i){
            if (name.equals(tdItems.get(i).fieldName)){
                return i;
            }
        }
        throw new NoSuchElementException("no field with a matching name is found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem item : tdItems) {
            size += item.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        if (td2==null){
           return td1;
        }
        if (td1==null){
           return td2;
        }
        TupleDesc res = new TupleDesc();
        for( int i = 0; i < td1.tdItems.size();++i){
           res.tdItems.add(td1.tdItems.get(i));
        }
        for (int i = 0; i < td2.tdItems.size();++i){
           res.tdItems.add(td2.tdItems.get(i));
        }
        return res;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if(!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc other = (TupleDesc) o;
        if (other.getSize() != this.getSize() || other.numFields() != this.numFields()) {
            return false;
        }

        for (int i = 0; i < this.numFields(); ++i) {
            if (!this.getFieldType(i).equals(other.getFieldType(i))) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        int res = 0;
        for (TDItem item : tdItems) {
            res += item.toString().hashCode()*41;
        }
        return res;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (TDItem item : tdItems) {
            sb.append(item.fieldType.toString()).append("(")
                    .append(item.fieldName).append("),");
        }
        sb.deleteCharAt(sb.length()-1);
        return sb.toString();
    }
}