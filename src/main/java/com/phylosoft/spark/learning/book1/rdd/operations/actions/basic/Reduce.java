package com.phylosoft.spark.learning.book1.rdd.operations.actions.basic;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaRDD;

public class Reduce {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("Reduce").run((jsc) -> {
            JavaRDD<Integer> lineLengths = jsc
                    .textFile("data/phylosoft/data.txt", 4)
                    .map(String::length);
            int totalLength = lineLengths
                    .reduce((a, b) -> a + b);

            System.out.println(totalLength);
        });

    }

}
