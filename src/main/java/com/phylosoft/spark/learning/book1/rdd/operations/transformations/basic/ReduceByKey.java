package com.phylosoft.spark.learning.book1.rdd.operations.transformations.basic;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

public class ReduceByKey {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("ReduceByKeyApp").run((jsc) -> {
            JavaPairRDD<String, Integer> pairs = jsc
                    .textFile("data/phylosoft/data.txt", 4)
                    .mapToPair(s -> new Tuple2(s, 1));

            JavaPairRDD<String, Integer> counts = pairs
                    .reduceByKey((a, b) -> a + b);
            counts.sortByKey();
            counts.collect();
        });

    }

}
