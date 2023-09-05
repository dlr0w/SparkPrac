package mini.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;

import java.util.Arrays;

public class wordcounts {
    public static void main(String[] args) {
        // SparkSessionの生成
        SparkConf conf = new SparkConf().setAppName("wordCount");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        String localFilePath = "file:///C:\\spark\\README.md";

        // 入力データのロード
        Dataset<String> input = spark.read().textFile(localFilePath);

        // 単語に分割
        Dataset<String> words = input.flatMap(line -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING());

        // 単語とカウントに変換
        Dataset<Row> counts = words.groupBy("value").count();

        // ワードカウントをテキストファイルに保存して、式が評価される。
        counts.write().text("C:\\Users\\sekai\\Downloads\\spark_output");

        // SparkSessionのクローズ
        spark.stop();
    }
}
