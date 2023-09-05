package mini.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class unionPrac {
    public static void main(String[] args) {
        // SparkSessionの生成
        SparkConf conf = new SparkConf().setAppName("wordCount");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        String localFilePath = "file:///C:\\spark\\README.md";

        // 入力データのロード
        Dataset<String> lines = spark.read().textFile(localFilePath);

        // Pythonという文字列がある行をフィルタリング
        Dataset<String> pythonLines = lines.filter(line -> line.contains("Python"));

        // Javaという文字列がある行をフィルタリング
        Dataset<String> javaLines = lines.filter(line -> line.contains("Java"));

        // PythonもしくはJavaを含む行を取得
        Dataset<String> javaAndPythonLines = pythonLines.union(javaLines);

        // 結果を表示
        javaAndPythonLines.collectAsList().forEach(line -> System.out.println("結果" + line));

        // sparksessionを閉じる
        spark.stop();
    }
}
