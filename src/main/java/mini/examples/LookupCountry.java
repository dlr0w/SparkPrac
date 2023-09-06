package mini.examples;
import static org.apache.spark.sql.functions.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

public class LookupCountry {

	//callsignテーブルを読み込むメソッド
	static String[] loadCallSignTable() throws FileNotFoundException {
	    Scanner callSignTbl = new Scanner(new File("./files/callsign_tbl_sorted"));
	    ArrayList<String> callSignList = new ArrayList<String>();
	    while (callSignTbl.hasNextLine()) {
	      callSignList.add(callSignTbl.nextLine());
	    }
	    return callSignList.toArray(new String[0]);
	  }

	static String lookupCountry(String callSign, String[] table) {
	      Integer pos = java.util.Arrays.binarySearch(table, callSign);
	      if (pos < 0) {
	        pos = -pos-1;
	      }
	      return table[pos].split(",")[1];
	  }

    public static void main(String[] args) throws FileNotFoundException {


        String sparkMaster = args[0];
        String inputFile = args[1];
        String outputDir = args[2];

        SparkSession spark = SparkSession.builder()
                .appName("ChapterSixExampleDataset")
                .getOrCreate();

        Dataset<String> file = spark.read().textFile(inputFile);

        // Count lines with KK6JKQ using accumulators
        LongAccumulator count = spark.sparkContext().longAccumulator();

        file.foreach(line -> {
            if (line.contains("KK6JKQ")) {
                count.add(1);
            }
        });
        System.out.println("Lines with KK6JKQ: " + count.value());

        // Count blank lines
        LongAccumulator blankLines = spark.sparkContext().longAccumulator();
        Dataset<String> callSigns = file.flatMap((line) -> {
            if (line.isEmpty()) {
                blankLines.add(1);
            }
            return Arrays.asList(line.split(" ")).iterator();
        }, Encoders.STRING());

        callSigns.write().text(outputDir + "/callsigns");
        System.out.println("Blank lines: " + blankLines.value());

        // Validate call signs
        LongAccumulator validSignCount = spark.sparkContext().longAccumulator();
        LongAccumulator invalidSignCount = spark.sparkContext().longAccumulator();
        Pattern pattern = Pattern.compile("^\\d?[a-zA-Z]{1,2}\\d{1,4}[a-zA-Z]{1,3}$");

        Dataset<String> validSigns = callSigns.filter((sign) -> {
            if (pattern.matcher(sign).matches()) {
                validSignCount.add(1);
                return true;
            } else {
                invalidSignCount.add(1);
                return false;
            }
        });

        Dataset<Row> contactCounts = validSigns.groupBy("value").count();
        contactCounts.count();

        if (invalidSignCount.value() < 0.1 * validSignCount.value()) {
            contactCounts.write().text(outputDir + "/contactCount");
        } else {
            System.out.println("Too many errors: " + invalidSignCount.value() + " in " + validSignCount.value());
        }

        //コールサインテーブルを読み込む
        //contactCounts内の各コールサインを読み込む
        //下記例ではうまくいかなかった。
        //Broadcast<String[]> signPrefixes = spark.sparkContext().broadcast(loadCallSignTable());

        //sparksesionでbroadcastを使用するためにsparkcontextを呼び出し
        SparkContext sc = spark.sparkContext();
        //ScalaとJava間に存在する違いを適切に処理するため、JavaSparkContextでラップする。
        JavaSparkContext jsc = new JavaSparkContext(sc);
        Broadcast<String[]> signPrefixes = jsc.broadcast(loadCallSignTable());


        Dataset<Row> countryContactCounts = contactCounts.map(new MapFunction<Row, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                String sign = row.getAs("value");
                String country = lookupCountry(sign, signPrefixes.value());
                Integer count = row.getAs("count");
                return new Tuple2<>(country, count);
            }
        }, Encoders.tuple(Encoders.STRING(), Encoders.INT()))
        .groupBy("_1").agg(sum("_2").as("count"));





        spark.close();
    }
}