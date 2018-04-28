package com.phylosoft.spark.learning.book1.tools.defaultinterface;

public interface Interface1 {

    default void hello() {
        System.out.println("Hello from Interface1");
    }

}