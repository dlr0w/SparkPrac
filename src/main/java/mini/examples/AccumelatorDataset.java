package mini.examples;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.util.LongAccumulator;

public class AccumelatorDataset {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("AccumulatorDatasetApp")
				.master("local[*]")
				.getOrCreate();

		Dataset<Row> ds = spark.read().text(args[1]);

		LongAccumulator blankLines = spark.sparkContext().longAccumulator();

		//ラムダ式ではできなかったのでflatMapFunctionを明示的に使ってコンパイルエラーを無くす。
		Dataset<String> words = ds.flatMap(new FlatMapFunction<Row, String>() {
			@Override
			public Iterator<String> call(Row row) throws Exception {
				String line = row.getString(0);
				if (line.equals("")) {
					blankLines.add(1);
				}
				return Arrays.asList(line.split(" ")).iterator();
			}
		}, Encoders.STRING());

		// flatmap関数が実行されて、アキュムレータが計算される。
		words.count();

		System.out.println("Blank lines: " + blankLines.value());

		//アキュムレータを作成
		LongAccumulator validSignCount = spark.sparkContext().longAccumulator();
		LongAccumulator invalidSignCount = spark.sparkContext().longAccumulator();

		Pattern p = Pattern.compile("\\A\\d?\\p{Alpha}{1,2}\\d{1,4}\\p{Alpha}{1,3}\\Z");

		Dataset<String> validCallSigns = words.filter((String sign) -> {
			Matcher matcher = p.matcher(sign);
			if (matcher.matches()) {
				validSignCount.add(1);
				return true;
			} else {
				invalidSignCount.add(1);
				return false;
			}
		});

		Dataset<Row> contactCounts = validCallSigns.groupBy("value")
				.agg(functions.count("value").as("count"));

		//件数を計算してカウンターに格納
		contactCounts.count();

		if (invalidSignCount.value() < 0.1 * validSignCount.value()) {
			contactCounts.write().csv("outputDir" + "/contactCount");
		} else {
			System.out.println("Too many errors " + invalidSignCount.value() +
					" for " + validSignCount.value());
			System.exit(1);
		}

		//        Dataset<String> validCallSigns = words.filter(
		//                new Function<String, Boolean>() {
		//                    @Override
		//                    public Boolean call(String ds) {
		//
		//                        Matcher m = p.matcher(ds);
		//                        boolean b = m.matches();
		//
		//                        if (b) {
		//                            validSignCount.add(1);
		//                        } else {
		//                            invalidSignCount.add(1);
		//                        }
		//                        return b;
		//                    }
		//                });

		spark.close();
	}
}
