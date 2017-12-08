package designPatterns.filter;

import java.io.IOException;

import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by pengzhang on 2017/5/18 0018.
 */
public class TopKMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private TreeMap<Integer, Text> sortMap = new TreeMap<>();

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Text item : this.sortMap.values()) {
            context.write(NullWritable.get(), item);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int compute = Integer.parseInt(value.toString().split("\t")[0]);

        this.sortMap.put(compute, value);

        if (this.sortMap.size() > 10) {
            this.sortMap.remove(this.sortMap.firstKey());
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
