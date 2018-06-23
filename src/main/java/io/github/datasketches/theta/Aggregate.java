package io.github.datasketches.theta;

import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.PairwiseSetOperations;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * Created by Andrew on 6/23/2018.
 *
 * Building one sketch using old Spark API:
 *
 */
public class Aggregate {

    public static void main(final String[] args) {
        final SparkConf conf = new SparkConf()
                .setAppName("Aggregate");
        final JavaSparkContext context = new JavaSparkContext(conf);

        final JavaRDD<String> lines = context.textFile("data/phylosoft/words.txt"); // one word per line

        final ThetaSketchJavaSerializable initialValue = new ThetaSketchJavaSerializable();
        final ThetaSketchJavaSerializable sketch = lines.aggregate(initialValue, new Add(), new Combine());

        System.out.println("Unique count: " + String.format("%,f", sketch.getEstimate()));
    }

    static class Add implements Function2<ThetaSketchJavaSerializable, String, ThetaSketchJavaSerializable> {
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