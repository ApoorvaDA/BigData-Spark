package org.assignment.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source

object mutualFriendInfo {
  
  def findMutualFriends(rdd1: RDD[String],rdd2: RDD[String]):Set[String]={ 
    val a= rdd1.map { friends => friends.split(",") }.collect().apply(0).toSet
    val b= rdd2.map { friends => friends.split(",") }.collect().apply(0).toSet
    
    a.intersect(b)
  }
  
  def main(args:Array[String]){
    var conf = new SparkConf().setAppName("MutualFriendInfo").setMaster("local[4]")
    var sc = new SparkContext(conf)
    
    val input_file = sc.textFile(args(0))
    val user_data_file = sc.textFile(args(1))
    
    var userA = args(2)
    var userB = args(3)
    
    val first_user_friends = input_file.filter(line => (line.split("\t")(0).equals(userA))).map(line => (line.split("\t")(1)))
    
    try {
      first_user_friends.isEmpty
    } catch {
      case e: Exception => print("User "+userA+" does not have any friends")
    }
   /* if(first_user_friends.isEmpty()){
      print("User "+userA+" does not have any friends")
    }else{
      first_user_friends.collect.foreach(println)
    }*/
    
    val second_user_friends = input_file.filter(line => (line.split("\t")(0).equals(userB))).map(line => (line.split("\t")(1)))
    if(second_user_friends.isEmpty()){
      print("User "+userB+" does not have any friends")
    }else{
      second_user_friends.collect.foreach(println)
    }
    
    val mutualFriends = findMutualFriends(first_user_friends,second_user_friends)
    if(mutualFriends.isEmpty){
      print(userA+" and "+userB+" do not have mutual friends")
    }else{  
      print(userA+"\t"+userB+"\t"+mutualFriends)
    }
    
    val mutualFriendList : List[String] = (for(x <- mutualFriends) yield x)(collection.breakOut)
    val mutualFriendRDD = sc.parallelize(mutualFriendList).map(x => (x,x))
    
    val required_data = user_data_file.map(line =>((line.split(",")(0)),(line.split(",")(1),line.split(",")(6))))
    val details = mutualFriendRDD.join(required_data)
    
    details.foreach((f:((String, (String, (String, String))))) => println((userA+"\t"+userB+"\t"+f._2._2)))

  }
}