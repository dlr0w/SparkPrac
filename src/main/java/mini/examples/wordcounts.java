package mini.examples;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class wordcounts {
	public static void main(String[] args) {
		//JavaのSpark Contextの生成
				SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
				JavaSparkContext sc = new JavaSparkContext(conf);
				String localFilePath = "file:///C:\\spark\\README.md";
				//入力データのロード
				JavaRDD<String> input = sc.textFile(localFilePath);
				//単語に分割
				JavaRDD<String> words = input.flatMap(
						new FlatMapFunction<String, String>() {
							private static final long serialVersionUID = 1L;
							public Iterator<String> call(String x){
								List<String> wordList = Arrays.asList(x.split(" "));
								return wordList.iterator();
							}
				});
				//単語とカウントに変換
				JavaPairRDD<String, Integer> counts = words.mapToPair(
						new PairFunction<String, String, Integer>() {
							private static final long serialVersionUID = 2L;
							public Tuple2<String, Integer> call(String x) {
								return new Tuple2(x, 1);
							}
						}).reduceByKey(new Function2<Integer, Integer, Integer>(){
							private static final long serialVersionUID = 3L;
							public Integer call(Integer x, Integer y) {
								return x + y;
							}
						});
				//ワードカウントをテキスト；ファイルに保存して、式が評価される。
				counts.saveAsTextFile("C:\\Users\\sekai\\Downloads\\spark_output");
	}
}
