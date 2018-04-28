package com.phylosoft.spark.learning.book1.rdd.sharedvariables.broadcasts;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Map;

public class MapSideJoinBroadcast {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("MapSideJoinBroadcast").run((jsc) -> {

            JavaPairRDD<String, String> userIdToCityId = jsc.parallelizePairs(
                    Arrays.asList(
                            new Tuple2<>("1", "101"),
                            new Tuple2<>("2", "102"),
                            new Tuple2<>("3", "107"),
                            new Tuple2<>("4", "103"),
                            new Tuple2<>("11", "101"),
                            new Tuple2<>("12", "102"),
                            new Tuple2<>("13", "107"),
                            new Tuple2<>("14", "103")));

            JavaPairRDD<String, String> cityIdToCityName = jsc.parallelizePairs(
                    Arrays.asList(
                            new Tuple2<>("101", "India"),
                            new Tuple2<>("102", "UK"),
                            new Tuple2<>("103", "Germany"),
                            new Tuple2<>("107", "USA")));

            Broadcast<Map<String, String>> citiesBroadcasted = jsc.broadcast(cityIdToCityName.collectAsMap());

            JavaRDD<Tuple3<String, String, String>> joined = userIdToCityId
                    .map(v1 -> new Tuple3<String, String, String>(v1._1(), v1._2(), citiesBroadcasted.value()
                            .get(v1._2())));

            System.out.println(joined.collect());

        });

    }
}
