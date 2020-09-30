package main.java.com.example.models;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.sql.Date;
import java.text.ParseException;

import static main.java.com.example.common.Constants.*;

@Data
@NoArgsConstructor
public class StockType implements Serializable {

    private Date date;
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Double volume;
    private Double adjClose;
    private String symbol;


    private String getStringValue(Result result, byte[] cf, byte[] identifier){
        final byte[] bytes = result.getValue(cf, identifier);
        String val = null;
        if(bytes != null) {
            val = new String(bytes);
        }
        return val;
    }


    private Date toSqlDate(String s){
        try {
            return new Date(utcDateFormat.parse(s).getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Double toDouble(String s){
        return Double.valueOf(s);
    }

    public StockType(Result result){

        this.date = toSqlDate(getStringValue(result, PRICE_CF, DATE));
        this.open = toDouble(getStringValue(result, PRICE_CF, OPEN));
        this.high = toDouble(getStringValue(result, PRICE_CF, HIGH));
        this.low = toDouble(getStringValue(result, PRICE_CF, LOW));
        this.close = toDouble(getStringValue(result, PRICE_CF, CLOSE));
        this.volume = toDouble(getStringValue(result, PRICE_CF, VOLUME));
        this.adjClose = toDouble(getStringValue(result, PRICE_CF, ADJCLOSE));
        this.symbol = getStringValue(result, INFO_CF, SYMBOL);
    }

    public byte[] toBytes(String s){
        if(s != null){
            return Bytes.toBytes(s);
        }
        return null;
    }

    public byte[] toBytes(Date d){
        if(d != null){
            return Bytes.toBytes(utcDateFormat.format(d));
        }
        return null;
    }

    public byte[] toBytes(Double d){
        if(d != null){
            return Bytes.toBytes(d);
        }
        return null;
    }

    public Double pct(){
        if(this.open != null && this.close != null){
            return 100 * (close - open) / open;
        }
        return null;
    }

    public Put toPut(){

        final Double pct = pct();

        final Put put = new Put(Bytes.toBytes(this.symbol + " " + this.date))
                .addColumn(PRICE_CF, DATE, toBytes(this.date))
                .addColumn(PRICE_CF, OPEN, toBytes(this.open))
                .addColumn(PRICE_CF, HIGH, toBytes(this.high))
                .addColumn(PRICE_CF, LOW, toBytes(this.low))
                .addColumn(PRICE_CF, CLOSE, toBytes(this.close))
                .addColumn(PRICE_CF, VOLUME, toBytes(this.volume))
                .addColumn(PRICE_CF, ADJCLOSE, toBytes(this.adjClose))
                .addColumn(INFO_CF, SYMBOL, toBytes(this.symbol))
                .addColumn(PRICE_CF, PCT, toBytes(pct))
                ;
        return put;
    }


}
