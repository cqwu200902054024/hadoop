package net.cqwu.designPatterns.filter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by pengzhang on 2017/5/18 0018.
 */
public class DistinctMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text hostSer = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String host = value.toString().split("\t")[25];
        this.hostSer.set(host);
        context.write(this.hostSer, NullWritable.get());
    }
}