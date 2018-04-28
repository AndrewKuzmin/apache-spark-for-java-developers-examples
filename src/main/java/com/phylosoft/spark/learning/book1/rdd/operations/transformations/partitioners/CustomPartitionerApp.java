package com.phylosoft.spark.learning.book1.rdd.operations.transformations.partitioners;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CustomPartitionerApp {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("CustomPartitionerApp").run((jsc) -> {

            JavaPairRDD<String, String> pairRdd = jsc.parallelizePairs(Arrays.asList(
                    new Tuple2<>("India", "Asia"),
                    new Tuple2<>("Germany", "Europe"),
                    new Tuple2<>("Japan", "Asia"),
                    new Tuple2<>("France", "Europe")), 3);

            JavaPairRDD<String, String> customPartitioned = pairRdd.partitionBy(new CustomPartitioner());
            System.out.println(customPartitioned.getNumPartitions());

            JavaRDD<String> mapPartitionsWithIndex = customPartitioned.mapPartitionsWithIndex((index, tupleIterator) -> {
                List<String> list = new ArrayList<>();
                while (tupleIterator.hasNext()) {
                    list.add("Partition number: " + index + ", key: " + tupleIterator.next()._1());
                }
                return list.iterator();
            }, true);
            System.out.println(mapPartitionsWithIndex.collect());

        });

    }

}
