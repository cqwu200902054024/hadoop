package net.cqwu.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by p.z on 2017/11/7 0007.
 */
public class MapReduceColorCount extends Configured implements Tool {
    public static void main(String[] agrs) throws Exception {
        int resCode = ToolRunner.run(new MapReduceColorCount(), agrs);

        if (resCode == 0) {
            System.out.println("SUCCESS");
        } else {
            System.out.println("ERROR");
        }

        System.exit(resCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());

        job.setJarByClass(MapReduceColorCount.class);
        job.setJobName(this.getClass().getSimpleName());
        job.setMapperClass(ColorCountMapper.class);
        job.setReducerClass(ColorCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));
        AvroJob.setInputKeySchema(job, User.getClassSchema());
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("target/record.avro"));
        FileOutputFormat.setOutputPath(job, new Path("target/out.avro"));

        return job.waitForCompletion(true)
               ? 0
               : 1;
    }

    public static class ColorCountMapper extends Mapper<AvroKey<User>, NullWritable, Text, IntWritable> {
        private static final IntWritable ONE   = new IntWritable(1);
        private Text                     color = new Text();

        @Override
        protected void map(AvroKey<User> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            this.color.set(key.datum().getFavoriteColor().toString());
            context.write(this.color, ONE);
        }
    }


    public static class ColorCountReducer
            extends Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Integer>(sum));
        }
    }
}

