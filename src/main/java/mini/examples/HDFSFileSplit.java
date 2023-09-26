package mini.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HDFSFileSplit {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("HDFS File Split").getOrCreate();

		//分割したいデータの読み込み
		Dataset<Row> data = spark.read()
				.option("delimiter", "\t") //タブ区切りを指定
				.option("header", "true")  //ヘッダー行の有無を指定
				.csv("hdfs:///user/sekai/tsv/tsvPrac.tsv");

		// データを4つのパーティーションに分割
		Dataset<Row> partitionedData = data.repartition(4);

		//HDFS上に保存
		partitionedData.write().csv("hdfs:///user/sekai/tsvSplit1");
		partitionedData.write().text("hdfs:///user/sekai/tsvSplit2");
	//	data.coalesce(4).write().text("hdfs:///user/sekai/tsvSplit");
	}
}
