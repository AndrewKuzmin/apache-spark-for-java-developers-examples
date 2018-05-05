package com.phylosoft.spark.learning.book1.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

@FunctionalInterface
public interface Runner {

    void start(JavaSparkContext jsc) throws Exception;

    default JavaRDD<String> loadTextFile(JavaSparkContext jsc) {
        JavaRDD<String> lines = jsc.textFile("data/phylosoft/data.txt", 4);
        System.out.println(lines.getNumPartitions());
        return lines;
    }

    static <K, V> void display(JavaPairRDD<K, V> rdd) {
        if (rdd != null) {
            List<Tuple2<K, V>> output = rdd.collect();
            for (Tuple2<?, ?> tuple : output) {
                System.out.println(tuple._1() + ": " + tuple._2());
            }
        }
    }

}
