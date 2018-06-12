package net.cqwu.mapreduce.lib;

import net.cqwu.io.Record;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBRecordReader;
import org.apache.hadoop.mapreduce.lib.db.MySQLDBRecordReader;
import org.apache.hadoop.mapreduce.lib.db.OracleDBRecordReader;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.mapreduce.lib
 *
 * @author： Administrator
 * Create Date：  2018-04-03
 * Modified By：   Administrator
 * Modified Date:  2018-04-03
 * Why & What is modified
 * Version:        V1.0
 */
public class DBInputFormat extends InputFormat<LongWritable,Record> implements Configurable {

    private String conditions;
    private Connection connection;
    private String tableName;
    private String[] fieldNames;
    private DBConfiguration dbConf;

    private String dbProductName = "DEFAULT";

    public DBInputFormat() {
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public RecordReader<LongWritable, Record> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void setConf(Configuration conf) {
         this.dbConf = new DBConfiguration(conf);
          try{
              this.getConnection();
              DatabaseMetaData dbMeta = this.connection.getMetaData();
             // this
          } catch (Exception e) {
              e.printStackTrace();
          }
    }

    @Override
    public Configuration getConf() {
        //
        return null;
    }

    public String getDbProductName() {
        return dbProductName;
    }

    protected  RecordReader<LongWritable,Record> createDBRecordReader(org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit split,Configuration conf) throws IOException{
        Class<Record> inputClass =(Class<Record>)this.dbConf.getInputClass();
        try{
            if(this.dbProductName.startsWith("ORACLE")) {
                return new OracleDBRecordReader<Record>(split,inputClass,conf,getConnection(),dbConf,conditions,fieldNames,tableName);
            } else if(this.dbProductName.startsWith("MYSQL")) {
                 return new MySQLDBRecordReader<Record>(split,inputClass,conf,getConnection(),dbConf,conditions,fieldNames,tableName);
            } else {
                return new DBRecordReader<Record>(split,inputClass,conf,getConnection(),dbConf,conditions,fieldNames,tableName);
            }
        } catch (SQLException ex) {
            throw new IOException(ex.getMessage());
        }
    }

   class DBInputSplit extends InputSplit implements Writable {
        private long end = 0;
        private long start = 0;

        public DBInputSplit() {
        }

        public DBInputSplit(long start,long end) {
            this.start = start;
            this.end = end;
        }

        public long getEnd() {
            return end;
        }

        public long getStart() {
            return start;
        }


        @Override
            public SplitLocationInfo[] getLocationInfo() throws IOException {
            //
            return super.getLocationInfo();
        }

        @Override
        public void write(DataOutput out) throws IOException {
             out.writeLong(this.start);
             out.writeLong(this.end);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
             this.start = in.readLong();
             this.end = in.readLong();
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return this.end - this.start;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }
    }

    public Connection getConnection() {
        try{
            if(this.connection == null) {
                this.connection = this.dbConf.getConnection();
                this.connection.setAutoCommit(false);
                this.connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            }
        } catch (Exception e) {
           e.printStackTrace();
        }
        return this.connection;
    }
}
