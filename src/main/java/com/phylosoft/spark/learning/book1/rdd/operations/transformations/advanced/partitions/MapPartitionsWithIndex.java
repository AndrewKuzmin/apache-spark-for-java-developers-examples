package com.phylosoft.spark.learning.book1.rdd.operations.transformations.advanced.partitions;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MapPartitionsWithIndex {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("MapPartitionsWithIndex").run((jsc) -> {
            JavaRDD<Integer> intRDD = jsc
                    .parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);

            JavaRDD<String> resultRDD = intRDD.mapPartitionsWithIndex((index, iterator) -> {
                List<String> list = new ArrayList<String>();
                while (iterator.hasNext()) {
                    list.add("Element " + iterator.next() + " belongs to partition " + index);
                }
                return list.iterator();
            }, false);

            List<String> output = resultRDD.collect();
            for (String item : output) {
                System.out.println(item);
            }

        });

    }

}
