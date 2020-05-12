package main.java.com.example;

import main.java.com.example.models.StockType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HbaseApp implements Serializable {

    private transient SparkSession sparkSession;
    private transient JavaSparkContext sparkContext;

    public void start(){
        SparkConf conf = new SparkConf();
        conf.setAppName(getClass().getName());
        conf.setIfMissing("spark.master", "local[2]");
        sparkSession = SparkSession.builder().config(conf).getOrCreate();

        sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        final Configuration hbaseConfig = getHbaseConfig();

        read("stocks");
    }

    private Configuration getHbaseConfig(){
        final Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", "localhost:2181");
        hbaseConfig.setInt("hbase.client.scanner.caching", 10000);
        hbaseConfig.set("hbase.zookeeper.quorum", "192.168.1.18:2181");
        hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure");
        hbaseConfig.set("timeout", "120000");
        return hbaseConfig;
    }


    private void show(JavaRDD<?> rdd, int n){
        final List<?> list = rdd.take(n);
        for (Object rec : list) {
            System.out.println(rec);
        }

    }

    public void read(String tableName){
        final Configuration hbaseConfig = getHbaseConfig();
        hbaseConfig.set(TableInputFormat.INPUT_TABLE, tableName);
        final JavaPairRDD<ImmutableBytesWritable, Result> hbaseRdd =
                sparkContext.newAPIHadoopRDD(hbaseConfig
                , TableInputFormat.class
                , ImmutableBytesWritable.class
                , Result.class
        );

        final JavaRDD<Result> hbaseResultRdd = hbaseRdd.map(Tuple2::_2);
        final JavaRDD<StockType> stockTypeJavaRDD = hbaseResultRdd.map(StockType::new);

        show(stockTypeJavaRDD, 10);

        final Dataset<StockType> dataset = sparkSession
                .createDataset(stockTypeJavaRDD.rdd(), Encoders.bean(StockType.class));

        dataset.show(10, false);

        dataset.createOrReplaceTempView("stocks");
        sparkSession.sql("select symbol, avg(volume) avg_vol" +
                " from stocks where year(date) > 2010 " +
                " group by symbol order by avg_vol desc limit 10")
                .show(10, false);

        write(dataset,tableName);

        sparkSession.close();
    }


    private void write(final Dataset<StockType> stocks, String tableName){

        stocks.foreachPartition(partition -> {
            final Configuration hbaseConfig = getHbaseConfig();
            try {
                final Connection connection = ConnectionFactory.createConnection(hbaseConfig);
                final Table table = connection.getTable(TableName.valueOf(tableName));
                List<Put> puts = new ArrayList<>();
                while (partition.hasNext()) {
                    final StockType stockType = partition.next();
                    puts.add(stockType.toPut());
                }
                Object[] results = new Object[puts.size()];
                table.batch(puts, results);
                table.close();
                connection.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        });
    }

    public static void main(String... args){
        new HbaseApp().start();
    }
}
