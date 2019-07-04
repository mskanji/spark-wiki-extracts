package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


case class Q2_ShowLeagueStatsTask(bucket: String) extends Runnable {
  //object Testing extends App {
  private val session: SparkSession = SparkSession.builder().getOrCreate()

  import session.implicits._


  //override def run(): Unit = {
  import org.apache.spark.sql.Encoders
  override def run(): Unit = {
    val schema = Encoders.product[LeagueStanding].schema
    val standings = session.read.schema(schema).parquet(bucket + "/part-*").as[LeagueStanding].cache()

    // TODO Répondre aux questions suivantes en utilisant le dataset $standings
    //  standings
    // ...code...
    //  .show()

    // TODO Q1


    standings.createOrReplaceTempView("Football")
    // session.sql ("select * from Football")


    println("Utiliser createTempView sur $standings et, en sql, afficher la moyenne de buts par saison et " +
      "par championnat")

    session.sql("select league,season, avg(points) from Football  group By league,season ").show()


    // TODO Q2
    println("En Dataset, quelle est l'équipe la plus titrée de France ?")

    val df1 = standings.filter(($"league" === "Ligue 1") && ($"position" === 1)).groupBy($"team").count().select($"team", $"count".as("le nombre des titres"))
    val df2 = standings.select("team").filter(($"league" === "Ligue 1") && ($"position" === 1)).groupBy($"team").count().agg(max("count"))
    df1.join(df2).where($"le nombre des titres" === $"max(count)").select($"team", $"le nombre des titres").show()


    // TODO Q3
    println("En Dataset, quelle est la moyenne de points des vainqueurs sur les 5 différents championnats ?")

    val df3 = standings.filter(($"position" === 1)).groupBy($"League").avg("points").show(40)


    // TODO Q5 Ecrire une udf spark "decade" qui retourne la décennie d'une saison sous la forme 199X ?


    val Decenie: Int => String = { y =>
      val x = y / 10
      val expr = x.toString + "X"
      expr

    }
    import org.apache.spark.sql.functions.udf
    val upperUDF = udf(Decenie)
    standings.withColumn("Decade", upperUDF('season)).show(100)

    // TODO Q4
    println("En Dataset, quelle est la moyenne de points d'écart entre le 1er et le 10ème de chaque championnats " +
      "par décennie")

    val df4 = standings.filter(($"position" === 1)).withColumnRenamed("points", "points de 1")
    val df5 = standings.filter($"position" === 10).withColumnRenamed("points", "points de 10")

    val df6 = df5.join(df4, Seq("season", "league"))
    val df7 = df6.withColumn("difference de points ", when(col("points de 1").isNull, lit(0)).otherwise(col("points de 1")) - when(col("points de 10").isNull, lit(0)).otherwise(col("points de 10")))
    df7.withColumn("Decade", upperUDF('season)).groupBy("League", "Decade").avg("difference de points ").show(50)


  }
}

