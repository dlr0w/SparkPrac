package mini.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class IpLogCount {
    public static void main(String[] args) {
    	SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("IpLogCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        // テキストファイルからログエントリを読み込み、それらをApacheAccessLogオブジェクトにマッピング
        JavaDStream<String> logLines = jssc.textFileStream("path/to/your/logfile.log");
        JavaDStream<ApacheAccessLog> accessLogs = logLines.map(ApacheAccessLog::parseFromLogLine);


        // IPアドレスごとのアクセス回数を計算
        JavaPairDStream<String, Long> ipCounts = accessLogs
                .mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1L))
                .reduceByKey((count1, count2) -> count1 + count2);



        // 結果を表示
        ipCounts.foreachRDD(rdd -> {
        	System.out.println("IP カウント" + rdd.collect());
        });

        jssc.start();
        try {
        	//終了するまで待つ
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
    }
}

// ApacheAccessLogクラスのサンプル実装
class ApacheAccessLog {
    private String ipAddress;

    // getter, setterなどの必要なメソッドを追加
    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    // ログ行からApacheAccessLogオブジェクトを作成
    public static ApacheAccessLog parseFromLogLine(String logLine) {
        ApacheAccessLog log = new ApacheAccessLog();
        // ここでは、サンプルとして固定のIPアドレスを設定しています。
        log.setIpAddress("123.123.123.123");
        return log;
    }
}
