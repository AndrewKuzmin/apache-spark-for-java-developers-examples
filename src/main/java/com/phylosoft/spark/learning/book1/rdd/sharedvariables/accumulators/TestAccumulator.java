package com.phylosoft.spark.learning.book1.rdd.sharedvariables.accumulators;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

public class TestAccumulator {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("TestAccumulator").run((jsc) -> {

            LongAccumulator longAccumulator = jsc.sc().longAccumulator("ExceptionCounter");

            JavaRDD<String> textFile = jsc.textFile("data2/phylosoft/logFileWithException.log");
            textFile.foreach((VoidFunction<String>) line -> {
                if (line.contains("Exception")) {
                    longAccumulator.add(1);
                    System.out.println("The intermediate value in loop " + longAccumulator.value());
                }
            });
            System.out.println("The final value of Accumulator : " + longAccumulator.value());

            CollectionAccumulator<Long> collectionAccumulator = jsc.sc().collectionAccumulator();
            textFile.foreach((VoidFunction<String>) line -> {
                if (line.contains("Exception")) {
                    collectionAccumulator.add(1L);
                    System.out.println("The intermediate value in loop " + collectionAccumulator.value());
                }
            });
            System.out.println("The final value of Accumulator : " + collectionAccumulator.value());

            ListAccumulator listAccumulator = new ListAccumulator();
            jsc.sc().register(listAccumulator, "ListAccumulator");
            textFile.foreach((VoidFunction<String>) line -> {
                if (line.contains("Exception")) {
                    listAccumulator.add("1");
                    System.out.println("The intermediate value in loop " + listAccumulator.value());

                }
            });
            System.out.println("The final value of Accumulator : " + listAccumulator.value());

        });

    }

}
