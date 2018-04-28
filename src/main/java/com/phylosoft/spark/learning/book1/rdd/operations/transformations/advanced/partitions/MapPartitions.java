package com.phylosoft.spark.learning.book1.rdd.operations.transformations.advanced.partitions;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MapPartitions {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("MapPartitions").run((jsc) -> {
            JavaRDD<Integer> intRDD = jsc
                    .parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);

            intRDD.mapPartitions(iterator -> {
                List<Integer> intList = new ArrayList<>();
                while (iterator.hasNext()) {
                    intList.add(iterator.next() + 1);
                }
                return intList.iterator();
            });


        });

    }

}
