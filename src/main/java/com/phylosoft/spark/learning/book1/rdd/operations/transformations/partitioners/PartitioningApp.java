package com.phylosoft.spark.learning.book1.rdd.operations.transformations.partitioners;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.HashPartitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PartitioningApp {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("PartitioningApp").run((jsc) -> {

            String mode = (args.length > 0) ? args[0] : "hash";

            JavaPairRDD<Integer, String> pairRdd = jsc
                    .parallelizePairs(Arrays.asList(
                            new Tuple2<>(1, "A"),
                            new Tuple2<>(2, "B"),
                            new Tuple2<>(3, "C"),
                            new Tuple2<>(4, "D"),
                            new Tuple2<>(5, "E"),
                            new Tuple2<>(6, "F"),
                            new Tuple2<>(7, "G"),
                            new Tuple2<>(8, "H")), 3);
            System.out.println(pairRdd.getNumPartitions());

            JavaPairRDD<Integer, String> partitionedRdd = null;
            if (mode.equals("hash")) {
                partitionedRdd = pairRdd.partitionBy(new HashPartitioner(2));
                System.out.println(partitionedRdd.getNumPartitions());
            } else {
                RDD<Tuple2<Integer, String>> rdd = JavaPairRDD.toRDD(pairRdd);
                RangePartitioner rangePartitioner = new RangePartitioner(
                        4, rdd, true,
                        scala.math.Ordering.Int$.MODULE$,
                        scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
                partitionedRdd = pairRdd.partitionBy(rangePartitioner);
                System.out.println(partitionedRdd.getNumPartitions());
            }

            JavaRDD<String> mapPartitionsWithIndex = partitionedRdd.mapPartitionsWithIndex((index, tupleIterator) -> {
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
