package com.phylosoft.spark.learning.book1.rdd.operations.transformations.advanced.partitions;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MapPartitionsToPair {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("MapPartitionsToPair").run((jsc) -> {
            JavaRDD<Integer> intRDD = jsc
                    .parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);

            JavaPairRDD<String, Integer> pairRDD = intRDD.mapPartitionsToPair(t -> {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                while (t.hasNext()) {
                    int element = t.next();
                    list.add(element % 2 == 0 ? new Tuple2<String, Integer>("even", element) :
                            new Tuple2<String, Integer>("odd", element));
                }
                return list.iterator();
            });

            List<Tuple2<String, Integer>> output = pairRDD.collect();
            for (Tuple2<?, ?> tuple : output) {
                System.out.println(tuple._1() + ": " + tuple._2());
            }


        });

    }

}
