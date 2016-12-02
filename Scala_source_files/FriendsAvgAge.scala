package org.assignment.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source

object FriendsAvgAge {
  
  def main(args:Array[String]){
    
    val conf = new SparkConf().setAppName("FriendsAvgAge").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val userFriendsFile = args(0)
    val userDetailsFile = args(1)
        
    val userFriends = sc.textFile(userFriendsFile);
    val userDetails = sc.textFile(userDetailsFile);
        
    val userDetailsRdd = userDetails.map(line => line.split(",")).map (line => (line(0),(2016 - (line(9).split("/")(2)).toInt)))
    val userFriendsRdd = userFriends.map(line => (line.split("\t"))).map(line => (line(0),if(line.length == 2){line(1);}))
    val userAddressRdd = userDetails.map(line => line.split(",")).map (line => (line(0),line(3) +" " + line(4) +" " +line(5) + " " +line(5) + " " +line(6) + " " +line(7)))
        
    val joinedUserFrndsAndAge = userDetailsRdd.join(userFriendsRdd)
 
    val frndAgeMap = joinedUserFrndsAndAge flatMap {case (key,(intVal, anyVal)) => (anyVal.toString().split(",").map(x => (x,intVal)))}
        
    val noOfFrnds = frndAgeMap.countByKey().toArray
    val frndTotalAge = frndAgeMap.reduceByKey(_+_)
    val noOfFrndsRdd = sc.parallelize(noOfFrnds) 
        
    val mapOfFrndsCountAndAge = frndTotalAge.join(noOfFrndsRdd)

    val userFrndsAgeAvg = mapOfFrndsCountAndAge.map (x => (x._1, (x._2._1 / x._2._2))).sortBy(_._2,false).take(20)
        
    val userFrndsAgeAvgRdd = sc.parallelize(userFrndsAgeAvg)
    val finalOutputRdd = userAddressRdd.join(userFrndsAgeAvgRdd)
    finalOutputRdd.foreach((user:(String,(String,Long))) => println((user._1)+", "+user._2._1+"\t"+user._2._2))
  }
}