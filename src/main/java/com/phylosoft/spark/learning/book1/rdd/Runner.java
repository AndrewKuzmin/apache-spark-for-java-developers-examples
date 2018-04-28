package com.phylosoft.spark.learning.book1.rdd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

@FunctionalInterface
public interface Runner {

    void start(JavaSparkContext jsc) throws Exception;

    default JavaRDD<String> loadTextFile(JavaSparkContext jsc) {
        JavaRDD<String> lines = jsc.textFile("data/phylosoft/data.txt", 4);
        System.out.println(lines.getNumPartitions());
        return lines;
    }

}
