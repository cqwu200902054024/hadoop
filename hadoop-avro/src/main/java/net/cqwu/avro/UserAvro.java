package net.cqwu.avro;

import java.io.File;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Created by p.z on 2017/11/7 0007.
 */
public class UserAvro {
    public static void main(String[] args) throws Exception {
        User user1 = new User();

        user1.setName("name1");
        user1.setFavoriteNumber(235);

        User user2 = new User("ben", 7, "red");
        User user3 = User.newBuilder().setName("char").setFavoriteColor("blue").setFavoriteNumber(12).build();

        // ���л�
        DatumWriter<User>    userDatumWriter = new SpecificDatumWriter<>(User.class);
        DataFileWriter<User> dataFileWriter  = new DataFileWriter<>(userDatumWriter);

        dataFileWriter.create(user1.getSchema(), new File("target/user.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();


        DatumReader<User>    userDatumReader = new SpecificDatumReader<>(User.class);
        DataFileReader<User> dataFileReader  = new DataFileReader<User>(new File("target/user.avro"), userDatumReader);
        User                 user            = null;

        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }
}


