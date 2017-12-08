package net.cqwu.avro;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

/**
 * Avro
 * Created by p.z on 2017/11/7 0007.
 */
public class AvroSchema {
    public static void main(String[] args) throws IOException {
        InputStream   inputStream = ClassLoader.getSystemResourceAsStream("user.avsc");
        Schema        schema      = new Schema.Parser().parse(inputStream);
        GenericRecord user        = new GenericData.Record(schema);

        /**
         * {"name": "name", "type": "string"},
         * {"name": "favorite_number",  "type": ["int", "null"]},
         * {"name": "favorite_color", "type": ["string", "null"]}
         */
        user.put("name", "p.z");
        user.put("favorite_number", 123);
        user.put("favorite_color", "blue");

        DatumWriter<GenericRecord>    datumWriter    = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

        dataFileWriter.create(schema, new File("target/record.avro"));
        dataFileWriter.append(user);
        dataFileWriter.close();


        DatumReader<GenericRecord>    datumReader    = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File("target/record.avro"),
                                                                                         datumReader);
        GenericRecord record = null;

        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
            System.out.println(record);
        }

        dataFileReader.close();
    }
}

