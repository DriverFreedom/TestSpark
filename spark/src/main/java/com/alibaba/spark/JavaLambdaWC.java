package com.alibaba.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * java的函数式 开发
 */
public class JavaLambdaWC {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("javaLambWC");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //加载数据
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //将数据分割 打散
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //每个单词记为1
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(w -> new Tuple2<>(w, 1));
        //计数
        JavaPairRDD<String, Integer> redeced = wordAndOne.reduceByKey((x, y) -> x + y);
        //颠倒顺序
        JavaPairRDD<Integer, String> swaped = redeced.mapToPair(tp -> tp.swap());
        //排序      false是倒序
        JavaPairRDD<String, Integer> sorted = redeced.sortByKey(false);
        //将顺序再颠倒回来
        JavaPairRDD<Integer, String> result = sorted.mapToPair(tp -> tp.swap());
        //保存数据
        result.saveAsTextFile(args[1]);

        jsc.close();

    }
}
