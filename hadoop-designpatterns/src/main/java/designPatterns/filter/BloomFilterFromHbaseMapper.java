package designPatterns.filter;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

/**
 * Created by pengzhang on 2017/5/18 0018.
 */
public class BloomFilterFromHbaseMapper extends Mapper<Object, Text, Text, NullWritable> {
    private BloomFilter bloomFilter = new BloomFilter();
    private Table       htable      = null;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String idcard = value.toString().split("\t")[0];

        if (bloomFilter.membershipTest(new Key(idcard.getBytes()))) {

            //
            // xueshengs
        }
    }    ////364944 +

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[]           files = context.getCacheFiles();
        DataInputStream input = new DataInputStream(new FileInputStream(files[0].getPath()));

        this.bloomFilter.readFields(input);
        input.close();    //

        Configuration hconf = HBaseConfiguration.create();

        this.htable = new HTable(hconf, "");
    }
}    //


//~ Formatted by Jindent --- http://www.jindent.com
