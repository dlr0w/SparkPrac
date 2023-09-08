package mini.examples;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class LargeDatasetMerges {
	public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark Application")
                .getOrCreate();

        //大量のデータを読み取り、直後にfilterを行って、内容をほとんどフィルタリングして、内容を少し取り出す時、RDDやDatasetが
        //巨大だと、filterが返すDataset/RDDは親と同じサイズを持つので、空のパーティーションや小さなパーティーションになりかねない。
        //この場合、パーティーション数がすくないRDDになるようにパーティーションを合併し直せば、パフォーマンスを改善できるかもしれない。


        //数千ファイルにマッチするかもしれないワイルドカードの入力
        Dataset<String> input = spark.read().textFile("S3のURL");
        Integer originalNumPartitions = input.rdd().getNumPartitions();

        //ほとんどすべてのデータを除外するフィルタ
        Dataset<String> lines = input.filter(line -> line.startsWith("2014-10-17"));
        Integer filteredNumPartitions = lines.rdd().getNumPartitions();

        //キャッシング前にlines RDDを5個に減らし、Datasetをキャッシング
        Dataset<String> coalescedLines = lines.coalesce(5).cache();
        Integer coalescedNumPartitions = coalescedLines.rdd().getNumPartitions();

        //これ以降の分析は合併済みのRDDに対して行える。
        long lineCount = coalescedLines.count();
        System.out.println("Line count: " + lineCount);

        spark.stop();
    }
}
