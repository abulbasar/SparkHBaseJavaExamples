package main.java.com.example.models;

import lombok.Data;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;


@Data
public class StockType implements Serializable {

    private String date;
    private String open;
    private String high;
    private String low;
    private String close;
    private String volume;
    private String adjClose;
    private String symbol;


    private String getStringValue(Result result, byte[] cf, byte[] identifier){
        final byte[] bytes = result.getValue(cf, identifier);
        String val = null;
        if(bytes != null) {
            val = new String(bytes);
        }
        return val;
    }


    public StockType(Result result){

        final byte[] PRICE_CF = Bytes.toBytes("price");
        final byte[] INFO_CF = Bytes.toBytes("info");

        final byte[] DATE = Bytes.toBytes("date");
        final byte[] OPEN = Bytes.toBytes("open");
        final byte[] HIGH = Bytes.toBytes("high");
        final byte[] LOW = Bytes.toBytes("low");
        final byte[] CLOSE = Bytes.toBytes("close");
        final byte[] VOLUME = Bytes.toBytes("volume");
        final byte[] ADJCLOSE = Bytes.toBytes("adjClose");
        final byte[] SYMBOL = Bytes.toBytes("symbol");

        this.date = getStringValue(result, PRICE_CF, DATE);
        this.open = getStringValue(result, PRICE_CF, OPEN);
        this.high = getStringValue(result, PRICE_CF, HIGH);
        this.low = getStringValue(result, PRICE_CF, LOW);
        this.close = getStringValue(result, PRICE_CF, CLOSE);
        this.volume = getStringValue(result, PRICE_CF, VOLUME);
        this.adjClose = getStringValue(result, PRICE_CF, ADJCLOSE);
        this.symbol = getStringValue(result, INFO_CF, SYMBOL);
    }

    public byte[] toBytes(String s){
        if(s != null){
            return Bytes.toBytes(s);
        }
        return null;
    }

    public Put toPut(){
        final byte[] PRICE_CF = Bytes.toBytes("price");
        final byte[] INFO_CF = Bytes.toBytes("info");


        final byte[] DATE = Bytes.toBytes("date");
        final byte[] OPEN = Bytes.toBytes("open");
        final byte[] HIGH = Bytes.toBytes("high");
        final byte[] LOW = Bytes.toBytes("low");
        final byte[] CLOSE = Bytes.toBytes("close");
        final byte[] VOLUME = Bytes.toBytes("volume");
        final byte[] ADJCLOSE = Bytes.toBytes("adjClose");
        final byte[] SYMBOL = Bytes.toBytes("symbol");


        final Put put = new Put(Bytes.toBytes(this.symbol + " " + this.date))
                .addColumn(PRICE_CF, DATE, toBytes(this.date))
                .addColumn(PRICE_CF, OPEN, toBytes(this.date))
                .addColumn(PRICE_CF, HIGH, toBytes(this.date))
                .addColumn(PRICE_CF, LOW, toBytes(this.date))
                .addColumn(PRICE_CF, CLOSE, toBytes(this.date))
                .addColumn(PRICE_CF, VOLUME, toBytes(this.date))
                .addColumn(PRICE_CF, ADJCLOSE, toBytes(this.date))
                .addColumn(INFO_CF, SYMBOL, toBytes(this.date))
                ;
        return put;
    }


}
