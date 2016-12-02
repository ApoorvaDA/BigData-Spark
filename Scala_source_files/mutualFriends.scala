package org.assignment.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source

object mutualFriends {
  
  def findMutualFriends(rdd1: RDD[String],rdd2: RDD[String]):Set[String]={ 
    val a= rdd1.map { friends => friends.split(",") }.collect().apply(0).toSet
    val b= rdd2.map { friends => friends.split(",") }.collect().apply(0).toSet
    
    a.intersect(b)
  }
  
  def main(args:Array[String]){
    
    var conf = new SparkConf().setAppName("MutualFriendFinder").setMaster("local[4]")
    var sc = new SparkContext(conf)
    
    val input_file = sc.textFile(args(0))
    
    var userA = args(1)
    var userB = args(2)
    
    val first_user_friends = input_file.filter(line => (line.split("\t")(0).equals(userA))).map(line => (line.split("\t")(1)))
    if(first_user_friends.isEmpty()){
      print("User "+userA+" does not have any friends")
    }
    
    val second_user_friends = input_file.filter(line => (line.split("\t")(0).equals(userB))).map(line => (line.split("\t")(1)))
    if(second_user_friends.isEmpty()){
      print("User "+userB+" does not have any friends")
    }
    
    val mutualFriends = findMutualFriends(first_user_friends,second_user_friends)
    if(mutualFriends.isEmpty){
      print(userA+" and "+userB+" do not have mutual friends")
    }else{  
      print(userA+"\t"+userB+"\t"+mutualFriends)
    }
  }
}