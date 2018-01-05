package net.cqwu.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.wordcount
 * @Author           p.z
 * Create Date  2017-12-08
 * Modified By   Administrator
 * Modified Date:  2017-12-08
 * Why & What is modified
 * Version:        V1.0
 */
public class WordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int resCode = ToolRunner.run(new WordCount(), args);

        if (resCode == 0) {
            System.out.println("SUCCESS!");
        } else {
            System.out.println("ERROR");
        }

        System.exit(resCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCount.class);
        job.setJobName(this.getClass().getSimpleName());
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setInputFormatClass(TextInputFormat.class);

        Path out =  new Path("D:\\data\\out\\test");
        FileSystem fs = FileSystem.get(configuration);
        fs.delete(out, true);
        FileInputFormat.setInputPaths(job, new Path("D:\\data\\in\\test"));
        FileOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)
               ? 0
               : 1;
    }
}
