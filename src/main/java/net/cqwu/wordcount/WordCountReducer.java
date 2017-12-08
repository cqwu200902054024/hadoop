package net.cqwu.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.wordcount
 * Author£º             Administrator
 * Create Date£º  2017-12-08
 * Modified By£º   Administrator
 * Modified Date:  2017-12-08
 * Why & What is modified
 * Version:        V1.0
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        this.outValue.set(sum);
        context.write(key, this.outValue);
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
