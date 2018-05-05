package com.phylosoft.spark.learning.book1.rdd.operations.transformations.advanced.aggregations;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.phylosoft.spark.learning.book1.rdd.Runner.display;

public class FoldByKey {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("FoldByKey").run((jsc) -> {

            JavaRDD<String> stringRDD = jsc.parallelize(Arrays.asList("Hello Spark", "Hello Java"));

            JavaPairRDD<String, Integer> flatMapToPair = stringRDD
                    .flatMapToPair(s -> Arrays.stream(s.split(" "))
                            .map(token -> new Tuple2<>(token, 1))
                            .collect(Collectors.toList()).iterator());

            JavaPairRDD<String, Integer> foldByKey = null;
            String mode = "1";
            switch (mode) {
                case "1":
                    foldByKey = flatMapToPair.foldByKey(0, (v1, v2) -> v1 + v2);
                    break;
                case "2":
//            foldByKey(V zeroValue, Partitioner partitione,rFunction2<V,V,V> func);
                    break;
                case "3":
//            foldByKey(V zeroValue, int numPartitions,Function2<V,V,V> func)
                    break;
            }

            display(foldByKey);

        });

    }

}
