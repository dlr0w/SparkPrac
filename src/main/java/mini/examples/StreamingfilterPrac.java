package mini.examples;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StreamingfilterPrac {
	//errorを含む行だけ、を出力する
    public static void main(String[] args) throws StreamingQueryException {
        // コマンドラインからマスターURLを取得
        String master = args[0];

        // SparkSessionを作成
        SparkSession spark = SparkSession
                .builder()
                .appName("StructuredNetworkWordCount")
                .master(master)
                .getOrCreate();

        // localhostの7777ポートからのストリームデータを読み取るデータフレームを作成
        Dataset<String> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 7777)
                .load()
                .as(Encoders.STRING());

        // "error"という文字列を含む行をフィルタリング
        Dataset<String> errorLines = lines.filter((FilterFunction<String>) line -> line.contains("error"));

        // エラーラインをコンソールに出力するストリームクエリを作成して開始
        StreamingQuery query = errorLines.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        // ストリームクエリが終了するまで待機
        query.awaitTermination();
    }
}
