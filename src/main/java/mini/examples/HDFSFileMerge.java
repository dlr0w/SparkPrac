package mini.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HDFSFileMerge {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("HDFS File Merge").getOrCreate();

		// 分割されたファイルを読み込む
        Dataset<Row> df = spark.read().option("delimiter", "\t").csv("hdfs:///user/sekai/tsvSplit1/*");

        // 1つのファイルに結合して保存
        df.coalesce(1).write().option("delimiter", "\t").csv("hdfs:///user/sekai/merged_tsvFile");
	}
}
