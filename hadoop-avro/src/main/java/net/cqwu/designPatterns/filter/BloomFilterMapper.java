package net.cqwu.designPatterns.filter;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import java.net.URI;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

/**
 * Created by pengzhang on 2017/5/17 0017.
 */
public class BloomFilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private BloomFilter bloomFilter = new BloomFilter();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String host = value.toString().split("\t")[25];

        if (bloomFilter.membershipTest(new Key(host.getBytes()))) {
            context.write(new Text(host), NullWritable.get());
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[]           files = context.getCacheFiles();
        DataInputStream input = new DataInputStream(new FileInputStream(files[0].getPath()));

        bloomFilter.readFields(input);
        input.close();
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
