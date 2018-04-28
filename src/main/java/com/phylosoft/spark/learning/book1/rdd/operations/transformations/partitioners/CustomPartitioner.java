package com.phylosoft.spark.learning.book1.rdd.operations.transformations.partitioners;

import org.apache.spark.Partitioner;

class CustomPartitioner extends Partitioner {

    private static final long serialVersionUID = -7397874438301367044L;

    private final int maxPartitions = 2;

    @Override
    public int numPartitions() {
        return maxPartitions;
    }

    @Override
    public int getPartition(Object key) {
        return (((String) key).length() % maxPartitions);
    }

}
