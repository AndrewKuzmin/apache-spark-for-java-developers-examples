package com.phylosoft.spark.learning.book1.rdd.operations.transformations.advanced.partitions;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static com.phylosoft.spark.learning.book1.rdd.Runner.display;

public class RepartitionAndSortWithinPartitions {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("RepartitionAndSortWithinPartitions").run((jsc) -> {

            List<Tuple2<Integer, String>> data = Arrays.asList(
                    new Tuple2<>(8, "h"),
                    new Tuple2<>(5, "e"),
                    new Tuple2<>(4, "d"),
                    new Tuple2<>(2, "a"),
                    new Tuple2<>(7, "g"),
                    new Tuple2<>(6, "f"),
                    new Tuple2<>(1, "a"),
                    new Tuple2<>(3, "c"));

            JavaPairRDD<Integer, String> unPartitionedRDD = jsc
                    .parallelizePairs(data);

            JavaPairRDD<Integer, String> resultRdd = unPartitionedRDD
                    .repartitionAndSortWithinPartitions(new HashPartitioner(2));

            display(resultRdd);

        });

    }

}
