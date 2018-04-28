package com.phylosoft.spark.learning.book1.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaApiRuntime {

    private SparkConf sparkConf;

    public JavaApiRuntime(String appName) {

        sparkConf = new SparkConf();
        sparkConf.setAppName(appName);

        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryoserializer.buffer", "24");
//        sparkConf.registerKryoClasses();
    }

    public void run(Runner runner) throws Exception {
        runner.start(new JavaSparkContext(sparkConf));
    }

}
