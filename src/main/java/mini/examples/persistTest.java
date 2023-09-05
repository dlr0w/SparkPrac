package mini.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class persistTest {
    public static void main(String[] args) {
        // SparkSessionの生成
        SparkConf conf = new SparkConf().setAppName("wordCount");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        String localFilePath = "file:///C:\\spark\\README.md";

        // 入力データのロード
        Dataset<String> lines = spark.read().textFile(localFilePath);

        // filter()変換の呼び出し
        Dataset<String> pythonLines = lines.filter(line -> line.contains("Python"));

        // Print the filtered lines (optional)
        pythonLines.collectAsList().forEach(line -> System.out.println("結果" + line));

        // Close the SparkSession
        spark.stop();
    }
}
