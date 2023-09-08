package mini.examples;

import org.apache.spark.SparkConf;

public class SparkSessionSetting {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();

		//アプリケーション名を設定
		conf.set("spark.app.name", "My Spark App");
		//masterURLを設定（Sparkアプリケーションがローカルモードで動作することを指示し、利用可能なコア数を4に設定しています)
		conf.set("spark.master", "local[4]");
		// SparkUIのポートを指定
		conf.set("spark.ui.port", "36000");

		//spark-submitでフラグを使った設定値の指定
		//$ bin/spark-submit \
		//	--master local[4] \
		//	--class com.example.MyApp \
		//	-name "My Spark App" \
		//	-conf spark.ui.port=36000 \

		//MyApp.jar

		/*
		 * デフォルトファイルを使った設定
		 * $ bin/spark-submit \
		--master local[4] \
		--properties-file my-config.conf \
		myApp.jar

		## my-config.confの内容
		spark.master local[4]
		spark.app.name "My Spark App"
		spark.ui.port 36000
		 */


	}
}
