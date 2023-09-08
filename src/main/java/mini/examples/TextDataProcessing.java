package mini.examples;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TextDataProcessing {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("TextDataProcessing").config("spark.ui.port", "4040").master("local[*]").getOrCreate();

        // 入力ファイルの読み取り
        Dataset<String> input = spark.read().textFile("./files/input.txt");

        // 単語への切り分けと空行の除去
        Dataset<Row> tokenized = input.filter(line -> !line.trim().isEmpty())
                .withColumn("value_split", split(col("value"), " "));

        // 最初の単語を取り出す
        Dataset<Row> words = tokenized.withColumn("words", element_at(col("value_split"), 1));

        // 出現回数をカウント
        Dataset<Row> counts = words.groupBy("words").count();
        
        // Dataset/RDDをキャッシュする
        counts.cache();
        
        counts.collect();
                
        counts.collect();
        
        
        //SparkUiで確認するために、Thread.sleepを入れる。
        try {
			Thread.sleep(1000000);
		} catch (InterruptedException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}

        spark.stop();
    }
}
