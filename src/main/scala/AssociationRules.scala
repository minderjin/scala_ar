/**
  * Created by lenovo on 2017-07-12.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD

object AssociationRules {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AssociationRules")
    val sc = new SparkContext(conf)

    val data = sc.textFile("./ar.basket")

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(','))

    val fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10)

    val model = fpg.run(transactions) // creates the FPGrowthModel

    val ar = new AssociationRules()
      .setMinConfidence(0.1)

    val results = ar.run(model.freqItemsets)


    println("count : " + results.count())

    results.collect().foreach { rule =>
      println("[" + rule.antecedent.mkString(",")
        + "=>"
        + rule.consequent.mkString(",") + "]," + rule.confidence)
    }

  }
}