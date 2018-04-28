package com.phylosoft.spark.learning.book1.tools.defaultinterface;

public interface Interface2 {

    default void hello() {
        System.out.println("Hello from Interface2");
    }

}