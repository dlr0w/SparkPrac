package mini.examples;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class parquetDataLoadPrac {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("ParquetDataLoadPrac").master("local[*]").enableHiveSupport()
				.getOrCreate();

		// Parquet ファイルからDatasetを作成
		Dataset<Row> rows = spark.read().parquet("file");


		 // "name"カラムを抽出して新しいDatasetを作成(複雑なロジックが必要な場合）
//		Dataset<String> names = rows.map(row -> {
//			return row.getAs("name");
//		}, Encoders.STRING());

		//↑と等価だが、catalystオプティマイザの利点を受けれ、パフォーマンスが高いかもしれない
		Dataset<String> names = rows.select("name").as(Encoders.STRING());


		// すべての名前をリストとして収集し、コンソールに出力
        List<String> nameList = names.collectAsList();
        System.out.println("Everyone");
        for(String name : nameList) {
            System.out.println(name);
        }

        //パンダが好きな人を探す。
        rows.createOrReplaceTempView("people");

        Dataset<Row> pandaFriends = spark.sql("SELECT name FROM people WHERE favouriteAnimal = \"panda\"");

        System.out.println("panda friends");

        System.out.println(pandaFriends.select("name").collect());


        //parquetファイルの保存
        pandaFriends.write().parquet("YourDirPath");


	}
}
