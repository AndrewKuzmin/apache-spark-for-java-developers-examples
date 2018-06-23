package io.github.datasketches.theta;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.yahoo.sketches.theta.PairwiseSetOperations;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.UpdateSketch;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import scala.Tuple2;

/**
 * Created by Andrew on 6/23/2018.
 *
 * Building multiple sketches (one sketch per key or dimension):
 *
 */
public class MapPartitionsToPairReduceByKey {

    static final ThetaSketchJavaSerializable emptySketchWrapped = new ThetaSketchJavaSerializable(UpdateSketch.builder().build().compact());

    public static void main(final String[] args) {
        final SparkConf conf = new SparkConf();
        final JavaSparkContext context = new JavaSparkContext(conf);

        final JavaRDD<String> lines = context.textFile("agg-by-key-input.txt"); // format: key\tvalue

        final JavaPairRDD<String, ThetaSketchJavaSerializable> mappedSketches = lines.mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<String>, String, ThetaSketchJavaSerializable>() {
                    @Override
                    public Iterator<Tuple2<String, ThetaSketchJavaSerializable>> call(final Iterator<String> input) {
                        // This map might be too big if there are too many keys in the input data
                        // One possible solution is to set a threshold on the number of entries
                        // and flush the HashMap once the threshold is reached (not shown here).
                        final Map<String, ThetaSketchJavaSerializable> map = new HashMap();
                        while (input.hasNext()) {
                            final String line = input.next();
                            final String[] tokens = line.split("\t");
                            ThetaSketchJavaSerializable sketch = map.get(tokens[0]);
                            if (sketch == null) {
                                sketch = new ThetaSketchJavaSerializable();
                                map.put(tokens[0], sketch);
                            }
                            sketch.update(tokens[1]);
                        }

                        final List<Tuple2<String, ThetaSketchJavaSerializable>> list = new ArrayList();
                        for (final Map.Entry<String, ThetaSketchJavaSerializable> entry: map.entrySet()) {
                            list.add(new Tuple2(entry.getKey(), entry.getValue()));
                        }
                        return list.iterator();
                    }
                }
        );

        final JavaPairRDD<String, ThetaSketchJavaSerializable> sketches = mappedSketches.reduceByKey(
                new Function2<ThetaSketchJavaSerializable, ThetaSketchJavaSerializable, ThetaSketchJavaSerializable>() {
                    @Override
                    public ThetaSketchJavaSerializable call(final ThetaSketchJavaSerializable sketch1, final ThetaSketchJavaSerializable sketch2) {
                        if (sketch1.getSketch() == null && sketch2.getSketch() == null) return emptySketchWrapped;
                        if (sketch1.getSketch() == null) return sketch2;
                        if (sketch2.getSketch() == null) return sketch1;
                        final CompactSketch compactSketch1 = sketch1.getCompactSketch();
                        final CompactSketch compactSketch2 = sketch2.getCompactSketch();
                        return new ThetaSketchJavaSerializable(PairwiseSetOperations.union(compactSketch1, compactSketch2));
                    }
                }, 1 // number of output partitions
        );

        final Iterator<Tuple2<String, ThetaSketchJavaSerializable>> it = sketches.toLocalIterator();
        while (it.hasNext()) {
            final Tuple2<String, ThetaSketchJavaSerializable> pair = it.next();
            System.out.println("Pair: (" + pair._1 + ", " + pair._2.getEstimate() + ")");
        }
    }

}