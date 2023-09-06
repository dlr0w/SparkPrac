package mini.examples;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;



public class ClosurePrac {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JdbcDatasetPreparedStatementApp").setMaster("local[*]");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();






		//失敗する

//		int counter = 0;
//		List<Integer> data = Arrays.asList(1, 2 ,3);
//
//		Dataset<Integer> ds = spark.createDataset(data, Encoders.INT());
//
//		ds.foreach(x -> counter += x);


		//データセットやRDDを使う際、クロージャに取り込まれた変数の書き換え
		//を行う場合、アキュムレータを使う必要がある

		//アキュムレータ
		LongAccumulator counter = spark.sparkContext().longAccumulator();

		List<Integer> data = Arrays.asList(1, 2 ,3);

		Dataset<Integer> ds = spark.createDataset(data, Encoders.INT());
		//アキュムレータの更新（通常の変数を使うとコンパイルエラー）
		ds.foreach(x -> counter.add(x));




	}
}
