package com.phylosoft.spark.learning.book1.tools.defaultinterface;

public class InterfaceImpl implements Interface1, Interface2 {

    @Override
    public void hello() {
        Interface1.super.hello();
        Interface2.super.hello();
    }

}

