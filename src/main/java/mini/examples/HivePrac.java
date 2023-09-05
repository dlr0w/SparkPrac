package mini.examples;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HivePrac {
	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("HivePrac").master("local[*]").enableHiveSupport()
				.getOrCreate();

		String jsonData = "{\"user\": {\"name\": \"Holden\", \"location\": \"San Francisco\"}, \"text\" : \"Nice day out today\"}\n"
				+ "{\"user\": {\"name\": \"Matei\", \"location\": \"Berkley\"}, \"text\" : \"Even nicer here :)\"}";

		// ファイルの保存先を指定
		String path = "C:\\\\Users\\\\sekai\\\\Downloads\\\\tweets.json";
		try (PrintWriter out = new PrintWriter(new File(path))) {
			out.print(jsonData);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// JSONファイルを読み込む
		Dataset<Row> tweets = spark.read().json(path);

		// DataFrameを一時ビューとして登録
		tweets.createOrReplaceTempView("tweets");

		// SQLクエリを実行
		Dataset<Row> results = spark.sql("SELECT user.name, text FROM tweets");
		results.show();

		spark.stop();
	}
}
