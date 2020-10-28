package com.spark.streaming



import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._
import java.util.concurrent._
import java.util.concurrent.atomic._

object popularhashtag {
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context named "popularhashtag" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "popularhashtag", Seconds(1))
    
    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())
    
    // Map this to tweet character lengths.
    val hashtag = statuses.flatMap(status => status.split(' ')).filter(rdd=>rdd.startsWith("#"))
    val hashtag_count=hashtag.map(rdd=>(rdd,1)).reduceByKeyAndWindow(_ + _,_ - _,Seconds(1),Seconds(1))
    val popularhastags=hashtag_count.transform(rdd=>rdd.sortBy(x=>x._2,false))
    popularhastags.print
    //batchDuration:  How often should the streaming context update?  how many seconds of data should each dstream contain?

//windowDuration:  What size windows are you looking for from this dstream?

//slideDuration:  Once I've given you that slice, how many units forward do you want me to move to give you the next one?
    //on basis of slide duration execution starts
       
    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}

