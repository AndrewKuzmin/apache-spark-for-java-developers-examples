package com.phylosoft.spark.learning.book1.rdd.operations.actions.advanced;

import com.phylosoft.spark.learning.book1.rdd.JavaApiRuntime;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class CollectAsyncApp {

    public static void main(String[] args) throws Exception {

        new JavaApiRuntime("CollectAsyncApp").run((jsc) -> {
            // Example of implementing a progress reporter for a simple job.
            JavaRDD<Integer> rdd = jsc
                    .parallelize(Arrays.asList(1, 2, 3, 4, 5), 5)
                    .map(new CollectAsyncApp.IdentityWithDelay<>());

            JavaFutureAction<List<Integer>> jobFuture = rdd.collectAsync();

            while (!jobFuture.isDone()) {
                Thread.sleep(1000);  // 1 second
                List<Integer> jobIds = jobFuture.jobIds();
                if (jobIds.isEmpty()) {
                    continue;
                }
                int currentJobId = jobIds.get(jobIds.size() - 1);
                SparkJobInfo jobInfo = jsc.statusTracker().getJobInfo(currentJobId);
                SparkStageInfo stageInfo = jsc.statusTracker().getStageInfo(jobInfo.stageIds()[0]);
                System.out.println(stageInfo.numTasks() + " tasks total: " + stageInfo.numActiveTasks() +
                        " active, " + stageInfo.numCompletedTasks() + " complete");
            }

            System.out.println("Job results are: " + jobFuture.get());
        });

    }

    public static final class IdentityWithDelay<T> implements Function<T, T> {
        @Override
        public T call(T x) throws Exception {
            Thread.sleep(2 * 1000);  // 2 seconds
            return x;
        }
    }

}
