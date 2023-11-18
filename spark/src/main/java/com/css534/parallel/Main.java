package com.css534.parallel;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import java.util.Arrays;
import java.util.Iterator;


public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Runner");
        SparkContext sc = new SparkContext(conf);

        int[] paralle = {1, 100, 20, 31};
    }
}