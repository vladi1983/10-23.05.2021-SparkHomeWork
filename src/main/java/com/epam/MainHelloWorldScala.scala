package com.epam

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}

object MainHelloWorldScala {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Hello spark from scala").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val fileTrips: RDD[String] = sc.textFile("data/taxi/trips.txt")
    fileTrips.persist(StorageLevel.MEMORY_AND_DISK)
    val fileDrivers: RDD[String] = sc.textFile("data/taxi/drivers.txt")
    fileDrivers.persist(StorageLevel.MEMORY_AND_DISK)

    val trips: RDD[Trips] = fileTrips.map(_.toLowerCase).map(_.split(" "))
      .map(_.toArray).map(x => Trips(x.apply(0).toInt, x.apply(1), x.apply(2).toInt))
    trips.persist(StorageLevel.MEMORY_AND_DISK)

    val drivers: RDD[Drivers] = fileDrivers.map(_.split(" ")).map(_.toArray)
      .map(x => Drivers(x.apply(0).toInt, x.apply(1)))

    //todo: Count number of lines in the file trips.txt
    println(s"Count number of lines: ${fileTrips.count()}")

    //todo: Count amount of trips to Boston longer than 10 km
    val amount = trips.filter(_.city.equals("boston")).filter(_.km > 10).count()
    println(s"Count amount of trips to Boston longer than 10 km: ${amount}")

    //todo: Calculate sum of all kilometers trips to Boston
    val kmToBoston: Double = trips.filter(_.city.equals("boston")).map(_.km).sum()
    println(s"Calculate sum of all kilometers trips to Boston: ${kmToBoston}")

    //todo: Write names of 3 drivers with max total kilometers in this day (sort top to down)
    val value1: RDD[(Int, String)] = drivers.map(x => (x.id, x.name))
    val value2: RDD[(Int, Int)] = trips.map(x => (x.id, x.km))
      .groupBy(x => x._1)
      .mapValues(x => x.map(_._2).sum)

    val tuples: Array[(Int, ((Int, String), (Int, Int)))] = value1.keyBy(_._1)
      .join(value2.keyBy(_._1))
      .sortBy(x => x._2._2, false).take(3)

    tuples.foreach(x => println("Three drivers with max total kilometers: " + x._2._1))

  }
}