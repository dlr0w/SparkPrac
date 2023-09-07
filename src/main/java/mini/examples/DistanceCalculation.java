//コールサインを取得するリンク先が死んでいたので、実行はしていません。

//package mini.examples;
//
//import static org.apache.spark.sql.functions.*;
//
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Iterator;
//import java.util.Scanner;
//import java.util.function.DoubleFunction;
//import java.util.regex.Pattern;
//
//import org.apache.spark.SparkContext;
//import org.apache.spark.SparkFiles;
//import org.apache.spark.api.java.JavaDoubleRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.MapFunction;
//import org.apache.spark.api.java.function.MapPartitionsFunction;
//import org.apache.spark.broadcast.Broadcast;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.util.LongAccumulator;
//import org.spark_project.jetty.client.HttpClient;
//import org.spark_project.jetty.client.api.ContentResponse;
//import org.spark_project.jetty.client.api.Request;
//
//import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import scala.Tuple2;
//
//public class DistanceCalculation {
//
//	//callsignテーブルを読み込むメソッド
//	static String[] loadCallSignTable() throws FileNotFoundException {
//		Scanner callSignTbl = new Scanner(new File("./files/callsign_tbl_sorted"));
//		ArrayList<String> callSignList = new ArrayList<String>();
//		while (callSignTbl.hasNextLine()) {
//			callSignList.add(callSignTbl.nextLine());
//		}
//		return callSignList.toArray(new String[0]);
//	}
//
//	static String lookupCountry(String callSign, String[] table) {
//		//binaryserchを使用する前に事前にソートしておく必要がある。
//		Integer pos = java.util.Arrays.binarySearch(table, callSign);
//		if (pos < 0) {
//			pos = -pos - 1;
//		}
//		return table[pos].split(",")[1];
//	}
//
//	static CallLog[] readExchangeCallLog(ObjectMapper mapper, ContentResponse response) {
//		try {
//			String responseJson = response.getContentAsString();
//			return mapper.readValue(responseJson, CallLog[].class);
//		} catch (Exception e) {
//			return new CallLog[0];
//		}
//	}
//
//	static Tuple2<String, Request> createRequestForSign(String sign, HttpClient client) throws Exception {
//		Request request = client.newRequest("http://new73s.herokuapp.com/qsos/" + sign + ".json");
//		return new Tuple2<>(sign, request);
//	}
//
//	static ObjectMapper createMapper() {
//		ObjectMapper mapper = new ObjectMapper();
//		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//		return mapper;
//	}
//
//	static Tuple2<String, CallLog[]> fetchResultFromRequest(ObjectMapper mapper,
//			Tuple2<String,Request> signExchange) {
//		String sign = signExchange._1();
//		Request exchange = signExchange._2();
//		return new Tuple2(sign, readExchangeCallLog(mapper, (ContentResponse) exchange));
//	}
//
//	public static void main(String[] args) throws Exception {
//
//		String inputFile = "./files/callsigns";
//		String inputFile2 = "./files/callsigns";
//		String outputDir = "C:\\\\Users\\\\sekai\\\\Downloads\\\\spark_output2";
//
//		SparkSession spark = SparkSession.builder()
//				.appName("ChapterSixExampleDataset")
//				.master("local[*]")
//				.getOrCreate();
//
//		Dataset<String> file = spark.read().textFile(inputFile);
//
//		// Count lines with KK6JKQ using accumulators
//		LongAccumulator count = spark.sparkContext().longAccumulator();
//
//		file.foreach(line -> {
//			if (line.contains("KK6JKQ")) {
//				count.add(1);
//			}
//		});
//		System.out.println("Lines with KK6JKQ: " + count.value());
//
//		// Count blank lines
//		LongAccumulator blankLines = spark.sparkContext().longAccumulator();
//		Dataset<String> callSigns = file.flatMap((line) -> {
//			if (line.isEmpty()) {
//				blankLines.add(1);
//			}
//			return Arrays.asList(line.split(" ")).iterator();
//		}, Encoders.STRING());
//
//		callSigns.write().text("C:\\\\Users\\\\sekai\\\\Downloads\\\\spark_output3" + "/callsigns");
//		System.out.println("Blank lines: " + blankLines.value());
//
//		// Validate call signs
//		LongAccumulator validSignCount = spark.sparkContext().longAccumulator();
//		LongAccumulator invalidSignCount = spark.sparkContext().longAccumulator();
//		Pattern pattern = Pattern.compile("^\\d?[a-zA-Z]{1,2}\\d{1,4}[a-zA-Z]{1,3}$");
//
//		Dataset<String> validSigns = callSigns.filter((sign) -> {
//			if (pattern.matcher(sign).matches()) {
//				validSignCount.add(1);
//				return true;
//			} else {
//				invalidSignCount.add(1);
//				return false;
//			}
//		});
//
//		Dataset<Row> contactCounts = validSigns.groupBy("value").count();
//		contactCounts.count();
//
//		if (invalidSignCount.value() < 0.1 * validSignCount.value()) {
//			contactCounts.javaRDD().saveAsTextFile(outputDir + "/contactCount");
//		} else {
//			System.out.println("Too many errors: " + invalidSignCount.value() + " in " + validSignCount.value());
//		}
//
//		//コールサインテーブルを読み込む
//		//contactCounts内の各コールサインを読み込む
//		//下記例ではうまくいかなかった。
//		//Broadcast<String[]> signPrefixes = spark.sparkContext().broadcast(loadCallSignTable());
//
//		//sparksesionでbroadcastを使用するためにsparkcontextを呼び出し
//		SparkContext sc = spark.sparkContext();
//		//ScalaとJava間に存在する違いを適切に処理するため、JavaSparkContextでラップする。
//		JavaSparkContext jsc = new JavaSparkContext(sc);
//		Broadcast<String[]> signPrefixes = jsc.broadcast(loadCallSignTable());
//
//		Dataset<Row> countryContactCounts = contactCounts.map(new MapFunction<Row, Tuple2<String, Long>>() {
//			@Override
//			public Tuple2<String, Long> call(Row row) throws Exception {
//				String sign = row.getAs("value");
//				String country = lookupCountry(sign, signPrefixes.value());
//				Long count = row.getAs("count");
//				return new Tuple2<>(country, count);
//			}
//		}, Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
//				.groupBy("_1").agg(sum("_2").as("count"));
//
//		countryContactCounts.javaRDD()
//				.saveAsTextFile("C:\\\\Users\\\\sekai\\\\Downloads\\\\spark_output4" + "/countries.txt");
//
//		Dataset<Tuple2<String, CallLog[]>> contactsContactLists = validSigns.mapPartitions(
//				new MapPartitionsFunction<String, Tuple2<String, CallLog[]>>() {
//					public Iterator<Tuple2<String, CallLog[]>> call(Iterator<String> input) throws Exception {
//						// List for our results.
//						ArrayList<Tuple2<String, CallLog[]>> callsignQsos = new ArrayList<>();
//						ArrayList<Tuple2<String, Request>> requests = new ArrayList<>();
//						ObjectMapper mapper = createMapper();
//						HttpClient client = new HttpClient();
//						try {
//							client.start();
//							while (input.hasNext()) {
//								requests.add(createRequestForSign(input.next(), client));
//							}
//							for (Tuple2<String, Request> signExchange : requests) {
//								callsignQsos.add(fetchResultFromRequest(mapper, signExchange));
//							}
//						} catch (Exception e) {
//							e.printStackTrace();
//						}
//						return callsignQsos.iterator();
//					}
//				}, Encoders.tuple(Encoders.STRING(), Encoders.bean(CallLog[].class)));
//
//
//
//		// 外部のRプログラムを使用して各コールの距離を計算します
//		// このジョブでダウンロードする各ノードのファイルリストにスクリプトを追加します
//		String distScript = System.getProperty("user.dir") + "/src/R/finddistance.R";
//		String distScriptName = "finddistance.R";
//		sc.addFile(distScript);
//
//
//		Dataset<CallLog[]> contactLogsDS = contactsContactLists.map((MapFunction<Row, CallLog[]>) row -> (CallLog[]) row.get(1), Encoders.bean(CallLog[].class));
//
//		Dataset<String> pipeInputs = contactLogsDS.flatMap((FlatMapFunction<CallLog[], String>) calls -> {
//		    ArrayList<String> latLons = new ArrayList<>();
//		    for (CallLog call : calls) {
//		        latLons.add(call.mylat + "," + call.mylong + "," + call.contactlat + "," + call.contactlong);
//		    }
//		    return latLons.iterator();
//		}, Encoders.STRING());
//
//		Dataset<String> distances = pipeInputs.sparkSession().createDataset(
//		    Arrays.asList(pipeInputs.javaRDD().pipe(SparkFiles.get(distScriptName)).collect().toArray(new String[0])),
//		    Encoders.STRING()
//		);
//
//		// 最初に、StringのRDDをDoubleRDDに変換して、統計関数にアクセス
//		JavaRDD<String> distancesRDD = distances.javaRDD();
//		JavaDoubleRDD distanceDoubles = distancesRDD.mapToDouble((DoubleFunction<String>) Double::parseDouble);
//
//
//		spark.close();
//	}
//}