package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final Integer[] buckets;
    private final int min;
    private final int max;
    private int sum;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.min = min;
    	this.max = max;
    	this.sum = 0;
    	this.buckets = new Integer[buckets];
        for (int i = 0; i < buckets; i++) {
            this.buckets[i] = 0;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int i = getBucketIndex(v);
        this.sum += 1;
        buckets[i]++;
    }
    private int getBucketIndex(int v){
    	int i = (v - min) * this.buckets.length / (max - min);
        if(i == this.buckets.length){
            i--;
        }
        return i;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	double res = 0;
        switch (op){
            case EQUALS:
            	res = this.equalSelectivity(v);
                break;
            case NOT_EQUALS:
                res = 1 - this.equalSelectivity(v);
                break;
            case LESS_THAN:
            	res = this.lessSelectivity(v);
            	break;
            case GREATER_THAN_OR_EQ:
                res = 1.0 - this.lessSelectivity(v);
                break;
            case LESS_THAN_OR_EQ:
                res = this.lessSelectivity(v) + equalSelectivity(v);
                break;
            case GREATER_THAN:
                res = 1 - this.lessSelectivity(v) - equalSelectivity(v);
                break;
        }
        return res;
    }

    private double lessSelectivity(final int v){
        int index = getBucketIndex(v);
        if(index < 0){
            return 0;
        }
        if(index >= this.buckets.length){
            return 1;
        }
        double i = this.buckets[index] * (this.buckets.length * (v - min) / (max - min) - index);
        for (int j = 0; j < index; j++) {
            i += this.buckets[j];
        }
        return i / sum;
    }

    private double equalSelectivity(final int v) {
        int index = getBucketIndex(v);
        if(index < 0 || index >= this.buckets.length){
        	return 0;
        }
        int bucket = this.buckets[index];
        return 1.0 * bucket / sum;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
