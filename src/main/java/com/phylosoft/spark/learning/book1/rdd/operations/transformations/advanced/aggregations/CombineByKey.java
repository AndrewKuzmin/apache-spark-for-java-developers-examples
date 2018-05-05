package com.phylosoft.spark.learning.book1.rdd.operations.transformations.advanced.aggregations;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Arrays;

import static com.phylosoft.spark.learning.book1.rdd.Runner.display;

public class CombineByKey {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("CombineByKey").run((jsc) -> {

            JavaPairRDD<String, String> pairRDD = jsc.parallelizePairs(Arrays.asList(
                    new Tuple2<>("key1", "Austria"),
                    new Tuple2<>("key2", "Australia"),
                    new Tuple2<>("key3", "Antartica"),
                    new Tuple2<>("key1", "Asia"),
                    new Tuple2<>("key2", "France"),
                    new Tuple2<>("key3", "Canada"),
                    new Tuple2<>("key1", "Argentina"),
                    new Tuple2<>("key2", "American Samoa"),
                    new Tuple2<>("key3", "Germany")), 3);

            JavaPairRDD<String, Integer> combineByKey = null;
            String mode = "1";
            switch (mode) {
                case "1":
                    combineByKey = pairRDD.combineByKey(
                            v1 -> {
                                if (v1.startsWith("A")) {
                                    return 1;
                                } else {
                                    return 0;
                                }
                            },
                            (v1, v2) -> {
                                if (v2.startsWith("A")) {
                                    v1 += 1;
                                }
                                return v1;
                            },
                            (v1, v2) -> v1 + v2);
                    break;
                case "2":
//            combineByKey(Function<V1,V2> createCombiner, Function2<V2,V1,V2> mergeValue,Function2<V2,V2,V2> mergeCombiners, Partitioner partitioner)
                    break;
                case "3":
//            combineByKey(Function<V1,V2> createCombiner, Function2<V2,V1,V2> mergeValue,Function2<V2,V2,V2> mergeCombiners, int numPartitions)
                    break;
                case "4":
//            combineByKey(Function<V1,V2> createCombiner, Function2<V2,V1,V2> mergeValue,Function2<V2,V2,V2> mergeCombiners, Partitioner partitioner, boolean mapSideCombine,Serializer serializer)
                    break;
            }

            display(combineByKey);

        });

    }

}

