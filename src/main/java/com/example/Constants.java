package main.java.com.example;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;

public class Constants implements Serializable {

    public static final byte[] PRICE_CF = Bytes.toBytes("price");
    public static final byte[] INFO_CF = Bytes.toBytes("info");

    public static final byte[] DATE = Bytes.toBytes("date");
    public static final byte[] OPEN = Bytes.toBytes("open");
    public static final byte[] HIGH = Bytes.toBytes("high");
    public static final byte[] LOW = Bytes.toBytes("low");
    public static final byte[] CLOSE = Bytes.toBytes("close");
    public static final byte[] VOLUME = Bytes.toBytes("volume");
    public static final byte[] ADJCLOSE = Bytes.toBytes("adjClose");
    public static final byte[] SYMBOL = Bytes.toBytes("symbol");
    public static final byte[] PCT = Bytes.toBytes("pct");
}
