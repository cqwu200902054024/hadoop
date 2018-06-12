package net.cqwu.io;

import org.apache.hadoop.io.*;

import java.io.*;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.io
 *
 * @author： Administrator
 * Create Date：  2018-04-02
 * Modified By：   Administrator
 * Modified Date:  2018-04-02
 * Why & What is modified
 * Version:        V1.0
 */
public class Tuple extends BinaryComparable implements WritableComparable<BinaryComparable> {
    private Writable[] values;
    private int size;

    private byte[] bytes;
    private int length;
    public Tuple() {
       this.values = new Writable[1];
       this.size = 1;
    }

    public Tuple(int n) {
        this.values = new Writable[n];
        this.size = n;
    }

    public Writable get(int i) {
        if(i < 0 || i >= size) {
            throw new ArrayIndexOutOfBoundsException(i);
        }
        return this.values[i];
    }

    public Writable set(int i,Writable value) {
        if(i < 0 || i >= this.size) {
            throw new ArrayIndexOutOfBoundsException(i);
        }
        return this.values[i] = value;
    }

    @Override
    public int compareTo(BinaryComparable other) {
        return super.compareTo(other);
    }

    @Override
    public int compareTo(byte[] other, int off, int len) {
        return super.compareTo(other, off, len);
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public int getLength() {
        return this.length;
    }

    @Override
    public byte[] getBytes() {
        ByteArrayInputStream bo;
        ObjectOutputStream oo;
        try{
            bo = new ByteArrayInputStream(this.bytes);
           // oo = new ObjectOutputStream();
            //oo.writeObject(this.values);
            this.length = this.bytes.length;
            //oo.close();
            bo.close();
            return this.bytes;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * A WritableComparator optimized for Text keys
     */
    public static class Comparator extends WritableComparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
              int n1 = WritableUtils.decodeVIntSize(b1[s1]);
              int n2 = WritableUtils.decodeVIntSize(b2[s2]);
              return compareBytes(b1, s1 + n1, l1 - n1, b2, s2 + n2, l2 - n2);
        }
    }

    static {
        WritableComparator.define(Tuple.class,new Comparator());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out,this.values.length);
        for(int i = 0; i < this.values.length;i ++) {
            Text.writeString(out,this.values[i].getClass().getName());
        }

        for(int i = 0;i < this.values.length; i ++) {
            this.values[i].write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.size = WritableUtils.readVInt(in);
        this.values = new Writable[this.size];
        Class<? extends Writable>[] cls = new Class[this.size];
        try {
           for(int i = 0; i < this.size; i ++) {
               cls[i] = Class.forName(Text.readString(in)).asSubclass(Writable.class);
           }

           for(int i = 0; i < this.size; i ++) {
               values[i] = cls[i].newInstance();
               values[i].readFields(in);
           }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
