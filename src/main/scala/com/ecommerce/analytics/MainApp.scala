package com.ecommerce.analytics

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object MainApp {

  def main(args: Array[String]): Unit = {

    // Question 7.1 : configuration externalisée (application.conf)
    val appConfig = ConfigFactory.load()

    val appName = appConfig.getString("app.name")
    val master = appConfig.getString("app.spark.master")
    val shufflePartitions = appConfig.getInt("app.spark.shuffle-partitions")

    val transactionsPath = appConfig.getString("app.data.input.transactions")
    val usersPath = appConfig.getString("app.data.input.users")
    val productsPath = appConfig.getString("app.data.input.products")
    val merchantsPath = appConfig.getString("app.data.input.merchants")
    val outputDir = appConfig.getString("app.data.output.directory")

    val sparkSession = SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.shuffle.partitions", shufflePartitions)
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    // Question 6.1 : orchestration complète du pipeline (ingestion -> transformation -> analytique -> sauvegarde)
    try {
      println(s"=== $appName : démarrage du pipeline ===")

      val ingestion = new DataIngestion(sparkSession)
      val transactions = ingestion.loadTransactions(transactionsPath)
      val users = ingestion.loadUsers(usersPath)
      val products = ingestion.loadProducts(productsPath)
      val merchants = ingestion.loadMerchants(merchantsPath)

      val transformation = new DataTransformation(sparkSession)
      val enriched = transformation.enrichTransactionData(transactions, users, products, merchants)

      // Question 5.1 : enriched est réutilisé 3x plus bas (rolling, rapport marchand, cohortes)
      // -> persist en MEMORY_AND_DISK_SER
      enriched.persist(StorageLevel.MEMORY_AND_DISK_SER)
      val enrichedCount = enriched.count() // matérialise le persist
      println(s"=== Transactions enrichies : $enrichedCount ligne(s) ===")

      val enrichedWithRolling = transformation.addRollingWindowFeatures(enriched)
      println("=== Aperçu transactions enrichies (montant cumulé 7j, utilisateur actif) ===")
      enrichedWithRolling.show(10, truncate = false)

      val analytics = new Analytics(sparkSession)

      val merchantReportDf = analytics.merchantReport(enriched)
      val cohortDf = analytics.cohortAnalysis(enriched)

      enriched.unpersist()

      merchantReportDf.cache() // réutilisé pour show() + 2 écritures
      cohortDf.cache()

      println("=== Rapport détaillé par marchand ===")
      merchantReportDf.show(20, truncate = false)

      println("=== Analyse de cohortes utilisateurs ===")
      cohortDf.show(50, truncate = false)

      println(s"=== Sauvegarde des résultats dans '$outputDir' ===")

      merchantReportDf.write.mode("overwrite").option("header", "true")
        .csv(s"$outputDir/merchant_report_csv")
      merchantReportDf.write.mode("overwrite")
        .parquet(s"$outputDir/merchant_report_parquet")

      cohortDf.write.mode("overwrite").option("header", "true")
        .csv(s"$outputDir/cohort_analysis_csv")
      cohortDf.write.mode("overwrite")
        .parquet(s"$outputDir/cohort_analysis_parquet")

      merchantReportDf.unpersist()
      cohortDf.unpersist()

      println("=== Pipeline terminé avec succès ===")

    } catch {
      case e: Exception =>
        println(s"=== ERREUR FATALE dans le pipeline : ${e.getMessage} ===")
        e.printStackTrace()
    } finally {
      sparkSession.stop()
    }
  }
}
