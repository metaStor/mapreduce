package secondarySort;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/*
 * Create By meta On 19-1-7 下午4:32
 */
public class IntPair implements WritableComparable<IntPair> {

    private IntWritable first;
    private IntWritable second;

    // 添加无参的构造方法,否则反射时会报错
    public IntPair() {
        set(new IntWritable(), new IntWritable());
    }

    public IntPair(IntWritable first, IntWritable second) {
        set(first, second);
    }

    public IntPair(int first, int second) {
        set(new IntWritable(first), new IntWritable(second));
    }

    private void set(IntWritable first, IntWritable second) {
        this.first = first;
        this.second = second;
    }

    public IntWritable getFirst() {
        return first;
    }

    public void setFirst(IntWritable first) {
        this.first = first;
    }

    public IntWritable getSecond() {
        return second;
    }

    public void setSecond(IntWritable second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
//        if (o == null || o instanceof IntPair) return false;
        IntPair intPair = (IntPair) o;
        return Objects.equals(first, intPair.first) &&
                Objects.equals(second, intPair.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    @Override
    public String toString() {
        return first +
                "\t" + second;
    }

    @Override
    public int compareTo(IntPair o) {
        // o the object to be compared
        int cmp = this.first.compareTo(o.first);
        if (cmp != 0) {
            return cmp;
        }
        return this.second.compareTo(o.second);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }
}
