package mini.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

public class HDFSFIleMove {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("HDFS File Move").getOrCreate();
		//HDFSファイル読み込み
		Path inputPath = new Path("hdfs:///user/sekai/hadoopPractice.txt");
		Path outputPath = new Path("hdfs:///user/sekai2/hadoopPractice.txt");

		//下記方法はI/Oが発生してしまうため、処理が重くなってしまう可能性がある
//		//HDFSへの書き込み
//		spark.read().text(inputPath).write().text(outputPath);



		//下記に置き換え
		//参考　https://sparkbyexamples.com/spark/spark-rename-and-delete-file-directory-from-hdfs/
		Configuration conf = spark.sparkContext().hadoopConfiguration();
		try {
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(inputPath) && fs.isFile(inputPath)) {

				// ファイルを移動する
	            boolean isMoved = fs.rename(inputPath, outputPath);
	            if (isMoved) {
	                System.out.println("成功");
	            } else {
	                System.out.println("失敗");
	            }

			}
		} catch (IOException e) {

			e.printStackTrace();
		}



		spark.stop();
	}
}
