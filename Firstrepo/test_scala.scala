package com.spark.streaming
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession


object test_scala {
  def main(args: Array[String]) {
      val spark=SparkSession.builder.appName("quickstart").master("local[*]").getOrCreate()
      val sc=spark.sparkContext
      val input = sc.parallelize(List(1, 2, 3, 4))
      val result = input.map(x => x * x)
      print(result.collect()(3))
}
}