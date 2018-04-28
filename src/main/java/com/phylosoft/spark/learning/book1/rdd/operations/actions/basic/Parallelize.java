package com.phylosoft.spark.learning.book1.rdd.operations.actions.basic;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.List;

public class Parallelize {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("ParallelizeApp").run((jsc) -> {
            List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
            JavaRDD<Integer> distData = jsc.parallelize(data);
        });

    }

}
