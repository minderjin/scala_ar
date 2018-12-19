/**
  * Created by lenovo on 2017-07-12.
  */
import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD


//import scalax.file.Path

object AssociationRules {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AssociationRules")
    val sc = new SparkContext(conf)

    val data = sc.textFile("/input/ar.basket")

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(','))

    val fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10)

    val model = fpg.run(transactions) // creates the FPGrowthModel

    // Confidence 설정
    val ar = new AssociationRules()
      .setMinConfidence(0.1)

    // RUN
    val results = ar.run(model.freqItemsets)

    // 결과 화면 출력
    println("#result count : " + results.count())
    results.collect().foreach { rule =>
      println("[" + rule.antecedent.mkString(",")
        + "=>"
        + rule.consequent.mkString(",") + "]:\t" + rule.confidence)
    }

    // 기존에 생성된 Directory(results) 삭제
    delete(new File("/output/results.1"))
    delete(new File("/output/results.2"))

    // 결과 파일 생성
    results.saveAsTextFile("/output/results.1")
    results.coalesce(1,true).saveAsTextFile("/output/results.2")

  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  def delete(file: File) {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete(_))
    file.delete
  }

//  def deletePath(pathStr: String): Unit = {
//    val path = Path.fromString(pathStr)
//    try {
//      path.deleteRecursively(continueOnFailure = false)
//    } catch {
//      case e: IOException => // some file could not be deleted
//    }
//  }
}