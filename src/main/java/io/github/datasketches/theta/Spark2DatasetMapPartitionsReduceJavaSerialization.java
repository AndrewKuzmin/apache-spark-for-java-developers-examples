package io.github.datasketches.theta;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.yahoo.sketches.theta.PairwiseSetOperations;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;
import com.yahoo.sketches.theta.CompactSketch;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.api.java.function.ReduceFunction;

import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import scala.Tuple2;
import java.util.Iterator;
import java.util.Arrays;

/**
 * Created by Andrew on 6/23/2018.
 *
 * Building one sketch using new Spark 2.x API and reading input from a Hive table:
 *
 */
public class Spark2DatasetMapPartitionsReduceJavaSerialization {

    public static void main(String[] args) {

        final SparkSession spark = SparkSession
                .builder()
                .appName("Spark2Aggregate")
                .enableHiveSupport()
                .getOrCreate();

        final Dataset<Row> data = spark.sql("select userid from my_data where userid is not null");

        final Dataset<ThetaSketchJavaSerializable> mappedData = data.mapPartitions(new MapPartitionsFunction<Row, ThetaSketchJavaSerializable>() {
            @Override
            public Iterator<ThetaSketchJavaSerializable> call(Iterator<Row> it) {
                ThetaSketchJavaSerializable sketch = new ThetaSketchJavaSerializable();
                while (it.hasNext()) {
                    sketch.update((String) it.next().get(0));
                }
                return Arrays.asList(sketch).iterator();
            }
        }, Encoders.javaSerialization(ThetaSketchJavaSerializable.class));

        final ThetaSketchJavaSerializable sketch = mappedData.reduce(new ReduceFunction<ThetaSketchJavaSerializable>() {
            @Override
            public ThetaSketchJavaSerializable call(ThetaSketchJavaSerializable sketch1, ThetaSketchJavaSerializable sketch2) throws Exception {
                if (sketch1.getSketch() == null && sketch2.getSketch() == null)
                    return new ThetaSketchJavaSerializable(UpdateSketch.builder().build().compact());
                if (sketch1.getSketch() == null) return sketch2;
                if (sketch2.getSketch() == null) return sketch1;
                final CompactSketch compactSketch1 = sketch1.getCompactSketch();
                final CompactSketch compactSketch2 = sketch2.getCompactSketch();
                return new ThetaSketchJavaSerializable(PairwiseSetOperations.union(compactSketch1, compactSketch2));
            }
        });

        System.out.println("Unique count: " + String.format("%,f", sketch.getEstimate()));

        spark.stop();
    }
}