package mini.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLPrac {
	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("SparkSQLPrac").master("local[*]").enableHiveSupport()
				.getOrCreate();
		String inputFile = "C:\\pleiades\\workspace\\SparkPrac\\files\\testweet.json";
//		String inputFile = "./files/testweet.json";

		//ツイートのロードとクエリ
		Dataset<Row> input = spark.read().json(inputFile);

		// 入力スキーマDatasetの登録
		//非推奨
		//input.registerTempTable("table");
		input.createOrReplaceTempView("tweets");

		// retweetCountを元にツイートを選択する。
		Dataset<Row> topTweets = spark.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10");


		//topTweetsのtext列へのJavaでのアクセス
		Dataset<String> topTweetText = topTweets.map(row -> {
			return row.getString(0);
		}, Encoders.STRING());


		//キャッシング
		input.createOrReplaceTempView("tweets");

		spark.sql("CACHE TABLE tweets");




	}
}
