package net.cqwu.parquet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 *
 * Parquet �Ķ�д����
 * Created by p.z on 2017/11/6 0006.
 */
public class ParquetTest {
    public static void main(String... args) throws Exception {

        // parquetWriter("D:\\data\\parquet\\parquet.parquet");
        parquetReaderV2("D:\\data\\parquet2\\part-r-00000.parquet");
    }

    public static void parquetReaderV2(String inPath) throws Exception {
        GroupReadSupport             readSupport = new GroupReadSupport();
        ParquetReader.Builder<Group> reader      = ParquetReader.builder(readSupport, new Path(inPath));
        ParquetReader<Group>         build       = reader.build();
        Group                        line        = null;

        while ((line = build.read()) != null) {
            Group time = line.getGroup("time", 0);

            System.out.println(time.getString("ttl2", 0));

            // ͨ���±���ֶ����ƻ�ȡ

            /*
             *      System.out.println("city: " + line.getString(0,0) + "\t" +
             *   "ip: " + line.getString(1,0) + "\t" +
             *    "tt1: " + time.getInteger(0,0) + "\t" +
             *    "ttl2: " + time.getString(1,0));
             */

            /*
             * System.out.println(line.getString("city",0) + "\t" +
             * line.getString("ip",0));
             */

            /*
             *  + "\t" +
             * time.getInteger("tt1",0) + "\t" +
             * time.getString("ttl2",0)
             */
        }
    }

    public static void parquetWriter(String outPath) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message Pair {\n" + " required binary city (UTF8);\n"
                                                                + " required binary ip (UTF8);\n"
                                                                + " repeated group time {\n" + " required int32 ttl;\n"
                                                                + " required binary ttl2;\n" + "}\n" + "}");

        System.out.println(schema);

        GroupFactory      factory       = new SimpleGroupFactory(schema);
        Path              path          = new Path(outPath);
        Configuration     configuration = new Configuration();
        GroupWriteSupport writeSupport  = new GroupWriteSupport();

        writeSupport.setSchema(schema, configuration);

        ParquetWriter<Group> writer = new ParquetWriter<Group>(path, configuration, writeSupport);

        for (int i = 0; i < 1000; i++) {
            Group group = factory.newGroup().append("city", "city" + i).append("ip", "192.168.1." + i);
            Group tmpG  = group.addGroup("time");

            tmpG.append("ttl", i * 10);
            tmpG.append("ttl2", i * 100 + "_a");

            Group tmpG2 = group.addGroup("time");

            tmpG2.append("ttl", i * 20);
            tmpG2.append("ttl2", i * 200 + "_a");
            writer.write(group);
        }

        writer.close();
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
