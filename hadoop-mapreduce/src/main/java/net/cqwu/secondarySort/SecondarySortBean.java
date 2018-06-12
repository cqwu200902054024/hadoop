package net.cqwu.secondarySort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.secondarySort
 *
 * @author： p.z
 * Create Date：  2018-03-28
 * Modified By：   p.z
 * Modified Date:  2018-03-28
 * Why & What is modified
 * Version:        V1.0
 */
public class SecondarySortBean implements WritableComparable<SecondarySortBean>{
    private IntWritable first;
    private IntWritable second;

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
    public String toString() {
        return "first:" + this.first.get() + "---second:" + this.second;
    }

    @Override
    public int compareTo(SecondarySortBean o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
