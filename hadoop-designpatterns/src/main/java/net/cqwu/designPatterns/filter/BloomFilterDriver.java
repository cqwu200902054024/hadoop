package net.cqwu.designPatterns.filter;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.zip.GZIPInputStream;

import static java.lang.Math.log;
import static java.lang.StrictMath.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * Created by p.z on 2017/5/17 0017.
 */
public class BloomFilterDriver {
    public static void main(String[] args) throws Exception {
        Path        inputFile    = new Path(args[0]);
        int         numNumbers   = Integer.parseInt(args[1]);
        float       falsePosRate = Float.parseFloat(args[2]);
        Path        bfile        = new Path(args[3]);
        int         vectorSize   = getOptimalBloomFilterSize(numNumbers, falsePosRate);
        int         nbHash       = getOptimalK(numNumbers, vectorSize);
        BloomFilter filter       = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);

        System.out.println(
            String.format(
                "Training Bloom Filter of size %d with %d hash functions,%d approx. no.records,and %f false pos. rate",
                vectorSize,
                nbHash,
                numNumbers,
                falsePosRate));

        String     line        = null;
        int        numElements = 0;
        FileSystem fs          = FileSystem.get(new Configuration());

        for (FileStatus status : fs.listStatus(inputFile)) {
            BufferedReader br =
                new BufferedReader(new InputStreamReader(new GZIPInputStream(fs.open(status.getPath()))));

            System.out.println("Reading" + status.getPath());

            while ((line = br.readLine()) != null) {
                filter.add(new Key(line.getBytes()));
                ++numElements;
            }

            br.close();
        }

        FSDataOutputStream fos = fs.create(bfile);

        filter.write(fos);
        fos.flush();
        fos.close();
    }

    public static int getOptimalBloomFilterSize(int n, double p) {
        return (int) ceil((n * log(p)) / log(1.0f / (pow(2.0f, log(2.0f)))));
    }

//  //////////////
    public static int getOptimalK(int n, int m) {    //
        return (int) round(log(2.0f) * m / n);
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
