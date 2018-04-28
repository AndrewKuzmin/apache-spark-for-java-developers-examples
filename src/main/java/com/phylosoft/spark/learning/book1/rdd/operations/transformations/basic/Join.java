package com.phylosoft.spark.learning.book1.rdd.operations.transformations.basic;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Arrays;

public class Join {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("Join").run((jsc) -> {
            JavaPairRDD<String, String> pairRDD1 = jsc.parallelizePairs(Arrays.asList(
                    new Tuple2<>("B", "A"),
                    new Tuple2<>("C", "D"),
                    new Tuple2<>("D", "E"),
                    new Tuple2<>("A", "B")));
            JavaPairRDD<String, Integer> pairRDD2 = jsc.parallelizePairs(Arrays.asList(
                    new Tuple2<>("B", 2),
                    new Tuple2<>("C", 5),
                    new Tuple2<>("D", 7),
                    new Tuple2<>("A", 8)));

            JavaPairRDD<String, Tuple2<String, Integer>> joinedRDD = pairRDD1.join(pairRDD2);
            System.out.println(joinedRDD.collect());

            pairRDD1.leftOuterJoin(pairRDD2);
            pairRDD1.rightOuterJoin(pairRDD2);
            pairRDD1.fullOuterJoin(pairRDD2);

//            join(JavaPairRDD<K,V> other,Partitioner partitioner)
//            leftOuterJoin(JavaPairRDD<K,V> other,Partitioner partitioner);
//            rightOuterJoin(JavaPairRDD<K,V> other,Partitioner partitioner);
//            fullOuterJoin(JavaPairRDD<K,V> other,Partitioner partitioner);


        });

    }

}
