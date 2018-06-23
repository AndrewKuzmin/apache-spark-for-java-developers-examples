package io.github.datasketches.theta;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import com.yahoo.sketches.theta.PairwiseSetOperations;
import com.yahoo.sketches.theta.UpdateSketch;
import com.yahoo.sketches.theta.CompactSketch;

import scala.Tuple2;

import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by Andrew on 6/23/2018.
 *
 * Building multiple sketches using SparkSession and reading input from a Hive table:
 *
 */
public class AggregateByKey2 {

    public static void main(String[] args) {

        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();

        SparkSession spark = SparkSession
                .builder()
                .appName("AggregateByKey2")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("CREATE TABLE IF NOT EXISTS my_data (id STRING, country STRING) USING hive");
        spark.sql("LOAD DATA LOCAL INPATH 'data/phylosoft/countries.txt' INTO TABLE my_data");

        Dataset<Row> data = spark.sql("select country, id from my_data where id is not null");
        data.show();

        final JavaPairRDD<String, String> pairs = data.javaRDD().mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<Row>, String, String>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(final Iterator<Row> input) {
                        final List<Tuple2<String, String>> list = new ArrayList();
                        while (input.hasNext()) {
                            final Row row = input.next();
                            list.add(new Tuple2<String, String>((String) row.get(0), (String) row.get(1)));
                        }
                        return list.iterator();
                    }
                }
        );

        final JavaPairRDD<String, ThetaSketchJavaSerializable> sketches = pairs.aggregateByKey(
                new ThetaSketchJavaSerializable(),
                1, // number of partitions
                new Add(),
                new Combine()
        );

        final Iterator<Tuple2<String, ThetaSketchJavaSerializable>> it = sketches.toLocalIterator();
        while (it.hasNext()) {
            final Tuple2<String, ThetaSketchJavaSerializable> pair = it.next();
            System.out.println("Pair: (" + pair._1 + ", " + pair._2.getEstimate() + ")");
        }

        spark.stop();
    }

    static class Add implements Function2<ThetaSketchJavaSerializable, String, ThetaSketchJavaSerializable> {
        @Override
        public ThetaSketchJavaSerializable call(final ThetaSketchJavaSerializable sketch, final String value) throws Exception {
            sketch.update(value);
            return sketch;
        }
    }

    static class Combine implements Function2<ThetaSketchJavaSerializable, ThetaSketchJavaSerializable, ThetaSketchJavaSerializable> {
        static final ThetaSketchJavaSerializable emptySketchWrapped = new ThetaSketchJavaSerializable(UpdateSketch.builder().build().compact());

        public ThetaSketchJavaSerializable call(final ThetaSketchJavaSerializable sketch1, final ThetaSketchJavaSerializable sketch2) throws Exception {
            if (sketch1.getSketch() == null && sketch2.getSketch() == null) return emptySketchWrapped;
            if (sketch1.getSketch() == null) return sketch2;
            if (sketch2.getSketch() == null) return sketch1;
            final CompactSketch compactSketch1 = sketch1.getCompactSketch();
            final CompactSketch compactSketch2 = sketch2.getCompactSketch();
            return new ThetaSketchJavaSerializable(PairwiseSetOperations.union(compactSketch1, compactSketch2));
        }
    }

}