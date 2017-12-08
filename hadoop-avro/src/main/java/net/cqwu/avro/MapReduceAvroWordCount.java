package net.cqwu.avro;

import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by p.z on 2017/11/7 0007.
 */
public class MapReduceAvroWordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int resCode = ToolRunner.run(new MapReduceAvroWordCount(), args);

        if (resCode == 0) {
            System.out.println("SUCCESS!");
        } else {
            System.out.println("ERROR");
        }

        System.exit(resCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(MapReduceAvroWordCount.class);
        job.setJobName(this.getClass().getSimpleName());
        job.setMapperClass(AvroWordCountMapper.class);
        job.setReducerClass(AvroWordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        AvroJob.setOutputKeySchema(job,
                                   Pair.getPairSchema(Schema.create(Schema.Type.STRING),
                                                      Schema.create(Schema.Type.INT)));
        job.setOutputValueClass(NullWritable.class);
        job.setSortComparatorClass(Text.Comparator.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("D:\\data\\in\\test"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\data\\out\\testavro"));

        return job.waitForCompletion(true)
               ? 0
               : 1;
    }

    public static class AvroWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE  = new IntWritable(1);
        private Text                     word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String          line      = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {
                this.word.set(tokenizer.nextToken());
                context.write(this.word, ONE);
            }
        }
    }


    public static class AvroWordCountReducer
            extends Reducer<Text, IntWritable, AvroWrapper<Pair<CharSequence, Integer>>, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(new AvroWrapper<>(new Pair<CharSequence, Integer>(key.toString(), sum)), NullWritable.get());
        }
    }
}

