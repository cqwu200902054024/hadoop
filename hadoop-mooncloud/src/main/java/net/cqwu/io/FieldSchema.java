package net.cqwu.io;

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
public class FieldSchema {
    public final String name;
    public final String type;
    public FieldSchema(String arg0,String arg1) {
        if("text".equals(arg1)) {
            arg1 = "string";
        } else if("bigint".equals(arg1)) {
            arg1 = "long";
        }

        name = arg0;
        type = arg1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(name);
        sb.append(":").append(type);
        return sb.toString();
    }
}
