package net.cqwu.parquet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

/**
 * Created by p.z on 2017/11/6 0006.
 */
public class ParquetMapReduce extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new ParquetMapReduce(), args);

        // LinkedList
        if (returnCode == 0) {
            System.out.println("SUCCESS!");
        } else {
            System.out.println("ERROR");
        }

        System.exit(returnCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf        = new Configuration();
        String        writeSchema = "message Pair {\n" + " required binary city (UTF8);\n"
                                    + " required binary ip (UTF8);\n" + " repeated group time {\n"
                                    + " required int32 ttl;\n" + " " + " required binary ttl2;\n" + "}\n" + "}";

        conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, writeSchema);

        Job job = Job.getInstance(conf);

        job.setJarByClass(ParquetMapReduce.class);
        job.setJobName("parquetTest");

        String in  = "D:\\data\\parquet\\parquet.parquet";
        String out = "D:\\data\\parquet2";

        job.setMapperClass(ParquetMapper.class);
        job.setReducerClass(ParquetOutPutReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Group.class);
        job.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        ParquetInputFormat.addInputPath(job, new Path(in));
        job.setOutputFormatClass(ParquetOutputFormat.class);
        ParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);
        FileOutputFormat.setOutputPath(job, new Path(out));

        return job.waitForCompletion(true)
               ? 0
               : 1;
    }

    public static class ParquetMapper extends Mapper<Void, Group, Text, Text> {
        private Text outKey   = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Void key, Group value, Context context) throws IOException, InterruptedException {
            String city = value.getString("city", 0);
            String ip   = value.getString("ip", 0);
            int    ttl  = value.getGroup("time", 0).getInteger("ttl", 0);
            String ttl2 = value.getGroup("time", 0).getString("ttl2", 0);

            this.outKey.set(city);
            this.outValue.set(ip + "\t" + ttl + "\t" + ttl2);
            context.write(this.outKey, this.outValue);
        }
    }


    public static class ParquetOutPutReducer extends Reducer<Text, Text, Void, Group> {
        private GroupFactory factory;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                String[] datas = value.toString().split("\t");
                String   ip    = datas[0];
                int      ttl   = Integer.valueOf(datas[1]);
                String   ttl2  = datas[2];
                Group    group = factory.newGroup().append("city", key.toString()).append("ip", ip);
                Group    time  = group.addGroup("time");

                time.append("ttl", ttl);
                time.append("ttl2", ttl2);
                context.write(null, group);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(context.getConfiguration()));
        }
    }


    public static class ParquetReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
