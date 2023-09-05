package mini.examples;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJDBC {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JdbcDatasetPreparedStatementApp").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        String url = "jdbc:mysql://localhost/test";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "your-username");//ユーザーネームを設定
        connectionProperties.put("password", "your-password");//パスワードを設定
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");// ドライバを設定

        // プリペアードステートメントを使用してデータをフェッチするためのクエリ
        String query = "SELECT * FROM panda WHERE id BETWEEN ? AND ?";

        // `predicates` はデータベースの各パーティションに対して実行するクエリの条件を指定します。
        // この場合、2つのパーティションにデータを分け1つ目が1～2　2つ目が3~4といった風に異なる範囲のクエリを実行できる。
        String[] numPartitions = {
            "id BETWEEN 1 AND 2",
            "id BETWEEN 3 AND 4"
        };

        Dataset<Row> data = spark.read()
            .jdbc(url, query, connectionProperties)
            .filter(numPartitions[0])
            .union(spark.read().jdbc(url, query, connectionProperties).filter(numPartitions[1]));

        data.show();

        spark.close();
    }
}
