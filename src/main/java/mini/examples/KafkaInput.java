package mini.examples;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public final class KafkaInput {
  public static void main(String[] args) throws Exception {
    // コマンドライン引数からKafkaの設定情報を取得
    String bootstrapServers = args[0];
    String group = args[1];
    String topics = args[2];

    // Kafkaの設定情報を保持するMapを作成
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", bootstrapServers);
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", group);
    kafkaParams.put("auto.offset.reset", "latest");
    kafkaParams.put("enable.auto.commit", false);

    // 購読するトピックのコレクションを作成
    Collection<String> topicsSet = Collections.singleton(topics);

    // Sparkの設定を作成し、JavaStreamingContextを初期化
    SparkConf conf = new SparkConf().setAppName("KafkaInput");
    JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));

    // Kafkaストリームを作成
    JavaInputDStream<ConsumerRecord<String, String>> stream =
      KafkaUtils.createDirectStream(
        jssc,
        LocationStrategies.PreferConsistent(),
        ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams)
      );

    // 受信したメッセージのkeyとvalueを結合してコンソールに出力
    stream.map(record -> record.key() + ": " + record.value()).print();

    // ストリーミングコンテキストを開始
    jssc.start();
    // ストリーミング処理が終了するまで待機(ここでは10秒後に停止)
    jssc.awaitTerminationOrTimeout(10000);
    // ストリーミングコンテキストを停止
    jssc.stop();
  }
}
