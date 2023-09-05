package mini.examples;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

public class AccumelatorErrorCount {
    public static void main(String[] args) {
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

        spark.close();
    }
}
