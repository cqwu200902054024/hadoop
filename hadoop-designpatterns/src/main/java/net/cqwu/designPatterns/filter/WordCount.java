package net.cqwu.designPatterns.filter;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by p.z on 2017/5/18 0018.
 */
public class WordCount extends Configured implements Tool {
    enum MyCounter { COUNTERA, COUNTERB }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new WordCount(), args);

        // LinkedList
        if (returnCode == 0) {
            System.out.println("SUCCESS!");
        } else {
            System.out.println("ERROR");
        }

        System.exit(returnCode);    //
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);

        List<String> list = new ArrayList<>();

        conf.setStrings("name", new String[] { "1", "2" });

        // conf.setClass("",(list).getClass(),);
        Collection<String> li = conf.getStringCollection("name");

        for (String l : li) {
            System.out.println(l + "===============>");
        }

        System.out.println("=============================================" + conf.getStrings("name")[1]);

        FileSystem fs = FileSystem.get(conf);

        // fs.re
        // fs.
        fs.delete(new Path(args[1]), true);

        Job job = Job.getInstance(conf);

        job.setJobName(this.getClass().getSimpleName());
        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.out.println("================>" + job.getGroupingComparator().getClass().getSimpleName());
        FileInputFormat.setInputPaths(job, "");

        // FileInputFormat.setInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // MultipleOutputs.addNamedOutput(job,"test",TextOutputFormat.class,Text.class,IntWritable.class);
        // MultipleOutputs.addNamedOutput(job,"other",TextOutputFormat.class,Text.class,IntWritable.class);
        int res = job.waitForCompletion(true)
                  ? 0
                  : 1;

        // System.out.println("==========================>A" + job.getCounters().findCounter(MyCounter.COUNTERA));
        // System.out.println("==========================>B" + job.getCounters().findCounter(MyCounter.COUNTERB).getValue());
        // org.apache.hadoop.mapreduce.Job.monitorAndPrintJob
        return res;
    }

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit split    = (FileSplit) context.getInputSplit();
            String    filename = split.getPath().toString();

            System.out.println("+++++++++++++++++++++++++++" + filename);

            String[] strs = value.toString().split(" ");

            if (value.toString().contains("test")) {
                context.getCounter(MyCounter.COUNTERA).increment(1);
            } else {
                context.getCounter(MyCounter.COUNTERB).increment(1);
            }

            for (String str : strs) {
                context.write(new Text(str), new IntWritable(1));
                context.write(new Text("sdsnodsnodnsod"), new IntWritable(1));
                context.getCounter("test", str).increment(1);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split    = (FileSplit) context.getInputSplit();
            String    filename = split.getPath().toString();

            System.out.println(filename);
        }
    }


    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private MultipleOutputs<Text, IntWritable> outputs = null;

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.outputs.close();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            String basePath = "";
            String named    = "";

            if (key.toString().contains("test")) {
                named    = "test1";
                basePath = "test";
            } else {
                named    = "other1";
                basePath = "other";
            }

            context.write(key, new IntWritable(sum));

            // outputs.write(key,new IntWritable(sum),basePath);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }
    }
}    ////


//~ Formatted by Jindent --- http://www.jindent.com
