package main.java.com.example;

import main.java.com.example.models.StockType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

public class HbaseApp {

    private SparkSession sparkSession;
    private JavaSparkContext sparkContext;
    private Configuration hbaseConfig;

    public void start(){
        SparkConf conf = new SparkConf();
        conf.setAppName(getClass().getName());
        conf.setIfMissing("spark.master", "local[2]");
        sparkSession = SparkSession.builder().config(conf).getOrCreate();

        sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", "localhost:2181");
        hbaseConfig.setInt("hbase.client.scanner.caching", 10000);
        hbaseConfig.set("hbase.zookeeper.quorum", "192.168.1.18:2181");
        hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure");
        hbaseConfig.set("timeout", "120000");

        read();
    }

    private void show(JavaRDD<?> rdd, int n){
        final List<?> list = rdd.take(n);
        for (Object rec : list) {
            System.out.println(rec);
        }

    }

    public void read(){
        hbaseConfig.set(TableInputFormat.INPUT_TABLE, "stocks");
        final JavaPairRDD<ImmutableBytesWritable, Result> hbaseRdd =
                sparkContext.newAPIHadoopRDD(hbaseConfig
                , TableInputFormat.class
                , ImmutableBytesWritable.class
                , Result.class
        );

        final JavaRDD<Result> hbaseResultRdd = hbaseRdd.map(Tuple2::_2);
        final JavaRDD<StockType> stockTypeJavaRDD = hbaseResultRdd.map(StockType::new);

        show(stockTypeJavaRDD, 10);

        final Dataset<Row> dataset = sparkSession.createDataFrame(stockTypeJavaRDD
                , StockType.class);

        dataset.show(10, false);

        dataset.createOrReplaceTempView("stocks");
        sparkSession.sql("select symbol, avg(volume) avg_vol" +
                " from stocks where year(date) > 2010 " +
                " group by symbol order by avg_vol desc limit 10")
                .show(10, false);
        sparkSession.close();
    }

    public static void main(String... args){
        new HbaseApp().start();
    }
}
