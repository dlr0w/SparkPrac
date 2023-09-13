package mini.examples;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetFromJavaBean {
	static class  HappyPerson implements Serializable{
		private String name;
		private String favouriteBeverage;
		public HappyPerson() {}
		public HappyPerson(String n, String b) {
			setName(n);
			setFavouriteBeverage(b);
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getFavouriteBeverage() {
			return favouriteBeverage;
		}
		public void setFavouriteBeverage(String favouriteBeverage) {
			this.favouriteBeverage = favouriteBeverage;
		}

	}
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("ParquetDataLoadPrac").master("local[*]").enableHiveSupport()
				.getOrCreate();

		ArrayList<HappyPerson> peopleList = new ArrayList<HappyPerson>();
		peopleList.add(new HappyPerson("holden", "coffee"));

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		JavaRDD<HappyPerson> happyPeopleRDD = sc.parallelize(peopleList);

		Dataset<Row> happyPeopleDs = spark.createDataFrame(happyPeopleRDD, HappyPerson.class);

		happyPeopleDs.createOrReplaceTempView("happy_people");
		
		
		
		
		
	}


}
