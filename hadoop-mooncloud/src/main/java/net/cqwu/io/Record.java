package net.cqwu.io;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.io
 *
 * @author： Administrator
 * Create Date：  2018-04-03
 * Modified By：   Administrator
 * Modified Date:  2018-04-03
 * Why & What is modified
 * Version:        V1.0
 */
public class Record implements Writable,DBWritable {
    private FieldSchema[] fields;
    private Writable[] values;
    private HashMap<String,Integer> fieldIndex;
    private int size;
    public Record() {}

    public Record(String schema) {
        this.schema(schema);
    }

    private void schema(String schema) {
        String[] s = schema.split(",",-1);
        this.fields = new FieldSchema[s.length];
        this.values = new Writable[s.length];
        this.fieldIndex = new HashMap<>(s.length);
        for(int i = 0; i < s.length; i ++) {
            String[] nt = s[i].split(":");
            String name = nt[0];
            String type = nt[1];
            this.fields[i] = new FieldSchema(name,type);
            this.fieldIndex.put(name,i);
        }
    }

    public int setAll(String schema,String[] value ) throws IOException{
        schema(schema);
        int size = setAll(value);
        return size;
    }

    public int setAll(String[] value) throws IOException {
        if(this.fields.length != value.length) {
            throw new IOException("expect" + fields.length + ", but the length is" + value.length);
        }
        if(this.values == null) {
            this.values = new Writable[this.fields.length];
        }
            int size = 0;
            for(int i = 0; i < fields.length; i ++) {
                if(value[i].startsWith("\"") && value[i].endsWith("\"")) {
                    value[i] = value[i].substring(1,value[i].length() - 1);
                }
                if("string".equals(fields[i].type)) {
                    this.values[i] = new Text(value[i]);
                } else if("long".equals(fields[i].type)) {
                    this.values[i] = new LongWritable(Long.parseLong(value[i]));
                } else if("double".equals(fields[i].type)) {
                    this.values[i] = new DoubleWritable(Double.parseDouble(value[i]));
                }
                size += value[i].getBytes().length;
            }
            return size;
        }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out,this.values.length);
        for(int i = 0; i < values.length; ++i) {
            Text.writeString(out,this.values[i].getClass().getName());
        }
        for(int i = 0; i < this.values.length; ++i) {
            this.values[i].write(out);
        }

        for(int i = 0; i < values.length; ++i) {
            Text.writeString(out,this.fields[i].name);
            Text.writeString(out,this.fields[i].type);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
         this.size = WritableUtils.readVInt(in);
         this.values = new Writable[this.size];
         Class<? extends Writable>[] cls = new Class[this.size];
         try {
             for(int i = 0; i < this.size; ++i) {
                 cls[i] = Class.forName(Text.readString(in)).asSubclass(Writable.class);
             }

             for(int i = 0; i < this.size; ++i) {
                 this.values[i] =cls[i].newInstance();
                 this.values[i].readFields(in);
             }
             this.fields = new FieldSchema[this.size];
             this.fieldIndex = new HashMap<>(this.size);
             for(int i = 0; i < this.size; ++i) {
                 this.fields[i] = new FieldSchema(Text.readString(in),Text.readString(in));
                 this.fieldIndex.put(fields[i].name,i);
             }
         } catch (ClassNotFoundException e) {
             throw (IOException) new IOException("Filed tuple init").initCause(e);
         } catch (IllegalAccessException e) {
             throw (IOException) new IOException("Failed tuple init").initCause(e);
         } catch (InstantiationException e) {
             throw (IOException) new IOException("Failed tuple init").initCause(e);
         }
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
         for(int i = 0; i < fields.length; i++) {
             if("string".equals(fields[i].type)) {
                 statement.setString(i + 1,((Text)values[i]).toString());
             } else if("long".equals(fields[i].type)) {
                 statement.setLong(i + 1,((LongWritable)values[i]).get());
             } else if("double".equals(fields[i].type)) {
                 statement.setDouble(i + 1,((DoubleWritable)values[i]).get());
             }
         }
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
          for(int i = 0; i < fields.length; i++) {
              if("string".equals(fields[i].type)) {
                  values[i] = new Text(resultSet.getString(i + 1));
              } else if("long".equals(fields[i].type)) {
                  values[i] = new LongWritable(resultSet.getLong(i + 1));
              } else if("double".equals(this.fields[i].type)) {
                  values[i] = new DoubleWritable(resultSet.getDouble(i + 1));
              }
          }
    }

    public Writable[] getAll() {
        return this.values;
    }

    public FieldSchema[] getFields() {
        return this.fields;
    }

    public FieldSchema getField(int i) {
        return this.fields[i];
    }
}
