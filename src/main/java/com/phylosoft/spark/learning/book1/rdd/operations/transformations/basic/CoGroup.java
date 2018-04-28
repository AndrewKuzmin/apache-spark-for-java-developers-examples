package com.phylosoft.spark.learning.book1.rdd.operations.transformations.basic;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Arrays;

public class CoGroup {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("CoGroup").run((jsc) -> {

            JavaPairRDD<String, String> pairRDD3 = jsc.parallelizePairs(
                    Arrays.asList(
                            new Tuple2<>("B", "A"),
                            new Tuple2<>("B", "D"),
                            new Tuple2<>("A", "E"),
                            new Tuple2<>("A", "B")));
            JavaPairRDD<String, Integer> pairRDD4 = jsc.parallelizePairs(
                    Arrays.asList(
                            new Tuple2<>("B", 2),
                            new Tuple2<>("B", 5),
                            new Tuple2<>("A", 7),
                            new Tuple2<>("A", 8)));

            JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = pairRDD3.cogroup(pairRDD4);
            System.out.println(cogroup.collect());

            JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> groupWith = pairRDD3.groupWith(pairRDD4);
            System.out.println(groupWith.collect());

//            cogroup(JavaPairRDD<K,V> other,Partitioner partitioner)


        });

    }

}
