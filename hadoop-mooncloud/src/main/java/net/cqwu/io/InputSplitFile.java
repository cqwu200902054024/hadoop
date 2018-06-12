package net.cqwu.io;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.io
 *
 * @author： p.z
 * Create Date：  2018-03-30
 * Modified By：   p.z
 * Modified Date:  2018-03-30
 * Why & What is modified
 * Version:        V1.0
 */
public class InputSplitFile implements WritableComparable<InputSplitFile> {
    private long offset;
    private String fileName;
    private String parentName;
    private String grandparentName;
    private String dt;
    private String tableName;
    private Path filePath;

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getParentName() {
        return parentName;
    }

    public void setParentName(String parentName) {
        this.parentName = parentName;
    }

    public String getGrandparentName() {
        return grandparentName;
    }

    public void setGrandparentName(String grandparentName) {
        this.grandparentName = grandparentName;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Path getFilePath() {
        return filePath;
    }

    public void setFilePath(Path filePath) {
        this.filePath = filePath;
    }

    /**
     *  序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.offset);
        out.writeUTF(this.fileName);
    }

    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.offset = in.readLong();
        this.fileName = in.readUTF();
    }

    @Override
    public int compareTo(InputSplitFile o) {
        InputSplitFile that = o;
        return this.fileName.compareTo(that.fileName)==0?(int)Math.signum((double)(this.offset - that.offset)) : this.fileName.compareTo(that.fileName);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof InputSplitFile) {
            return this.compareTo((InputSplitFile)obj) ==0;
        }
        return false;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
