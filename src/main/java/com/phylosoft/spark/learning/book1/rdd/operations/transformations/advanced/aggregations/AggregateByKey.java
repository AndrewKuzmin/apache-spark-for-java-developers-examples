package com.phylosoft.spark.learning.book1.rdd.operations.transformations.advanced.aggregations;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class AggregateByKey {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("AggregateByKey").run((jsc) -> {

            JavaPairRDD<String, String> pairRDD = jsc.parallelizePairs(Arrays.asList(
                    new Tuple2<String, String>("key1", "Austria"),
                    new Tuple2<String, String>("key2", "Australia"),
                    new Tuple2<String, String>("key3", "Antartica"),
                    new Tuple2<String, String>("key1", "Asia"),
                    new Tuple2<String, String>("key2", "France"),
                    new Tuple2<String, String>("key3", "Canada"),
                    new Tuple2<String, String>("key1", "Argentina"),
                    new Tuple2<String, String>("key2", "American Samoa"),
                    new Tuple2<String, String>("key3", "Germany")), 3);

            JavaPairRDD<String, Integer> aggregateByKey = null;
            String mode = "1";
            switch (mode) {
                case "1":
                    aggregateByKey = pairRDD.aggregateByKey(
                            0,
                            (v1, v2) -> {
                                System.out.println(v2);
                                if (v2.startsWith("A")) {
                                    v1 += 1;
                                }
                                return v1;
                            },
                            (v1, v2) -> v1 + v2);
                    break;
                case "2":
//            aggregateByKey(V2 zeroValue, Partitioner partitioner, Function2<V2,V1,V2> seqFunction,Function2<V2,V2,V2> combFunction)
                    break;
                case "3":
//            aggregateByKey(V2 zeroValue, int numPartitions, Function2<V2,V1,V2> seqFunction,Function2<V2,V2,V2> combFunction)
                    break;
            }

            if (aggregateByKey != null) {
                List<Tuple2<String, Integer>> output = aggregateByKey.collect();
                for (Tuple2<?, ?> tuple : output) {
                    System.out.println(tuple._1() + ": " + tuple._2());
                }
            }

        });

    }

}
