/**
 * leo: logEndOffset
 * lso: logStartOffset
 * lst: logStartTime
 * let: logEndTime
 */
package org.apache.kafka.common;

public class NewOffsetMetaData {
    public final long leo;
    public final int brokerid;
    public final long let;
    public final long lst;
    public final long lso;

    public NewOffsetMetaData(int brokerid, long leo, long lst, long let, long lso) {
        this.brokerid = brokerid;
        this.leo = leo;
        this.lst = lst;
        this.let = let;
        this.lso = lso;
    }

    public String toString() {
        java.text.SimpleDateFormat df = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String lstTime = df.format(this.lst);
        String letTime = df.format(this.let);
        return "[broker=" + this.brokerid + "]->[lst=" + this.lst + "(" + lstTime + "),lso=" + this.lso +
                ",let=" + this.let + "(" + letTime + "), leo=" + this.leo + "]";
    }
}