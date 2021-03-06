package com.phylosoft.spark.learning.book1.rdd.operations.actions.advanced;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.List;

public class TakeAsync {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("TemplateApp").run((jsc) -> {
            JavaRDD<Integer> intRDD1 = jsc.parallelize(Arrays.asList(1, 4, 3, 5, 7, 6, 9, 10, 11, 13, 16, 20), 4);
            JavaRDD<Integer> intRDD2 = jsc.parallelize(Arrays.asList(31, 34, 33, 35, 37, 36, 39, 310, 311, 313, 316, 320), 4);

            JavaFutureAction<List<Integer>> takeAsync = intRDD1.takeAsync(3);
            for (Integer val : takeAsync.get()) {
                System.out.println(" The async value of take is :: " + val);
            }


        });

    }

}
