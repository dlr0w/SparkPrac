package mini.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ApacheHIvePrac {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("SparkSQLPrac").master("local[*]").enableHiveSupport()
				.getOrCreate();

		Dataset<Row> rows = spark.sql("SLECT key, value  From mytable");

		Dataset<Integer> keys = rows.map(row -> {
			return row.getInt(0);
		}, Encoders.INT());

	}
}
