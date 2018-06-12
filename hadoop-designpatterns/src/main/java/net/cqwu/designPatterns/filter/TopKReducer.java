package net.cqwu.designPatterns.filter;

import java.io.IOException;

import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by pengzhang on 2017/5/18 0018.
 */
public class TopKReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
    private TreeMap<Integer, Text> treeMap = new TreeMap<>();

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Text text : this.treeMap.values()) {
            context.write(NullWritable.get(), text);
        }
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            int compute = Integer.parseInt(value.toString().split("\t")[0]);

            this.treeMap.put(compute, value);

            if (this.treeMap.size() > 10) {
                this.treeMap.remove(this.treeMap.firstKey());
            }
        }
    }
}