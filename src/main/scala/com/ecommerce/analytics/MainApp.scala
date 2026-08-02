package com.ecommerce.analytics

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * Application principale : orchestre tout le pipeline (Partie 6) en s'appuyant
 * sur la configuration externalisée d'application.conf (Partie 7).
 */
object MainApp {

  def main(args: Array[String]): Unit = {

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
      .config("spark.sql.adaptive.enabled", "true") // AQE (Chapitre 6 Optimisation Spark)
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR") // réduit les logs bruyants

    try {
      println(s"=== $appName : démarrage du pipeline ===")

      // ----- Partie 2 : Ingestion + validation -----
      val ingestion = new DataIngestion(sparkSession)
      val transactions = ingestion.loadTransactions(transactionsPath)
      val users = ingestion.loadUsers(usersPath)
      val products = ingestion.loadProducts(productsPath)
      val merchants = ingestion.loadMerchants(merchantsPath)

      // ----- Partie 3 : Transformations avancées (UDF + jointures + window functions) -----
      val transformation = new DataTransformation(sparkSession)
      val enriched = transformation.enrichTransactionData(transactions, users, products, merchants)

      // Partie 5.1 : `enriched` est réutilisé 3 fois ci-dessous (fenêtre glissante,
      // rapport marchand, cohortes) => persist en MEMORY_AND_DISK_SER (DataFrame
      // volumineux issu de jointures + UDF, potentiellement trop gros pour la RAM seule).
      enriched.persist(StorageLevel.MEMORY_AND_DISK_SER)
      val enrichedCount = enriched.count() // action qui matérialise le persist
      println(s"=== Transactions enrichies : $enrichedCount ligne(s) ===")

      val enrichedWithRolling = transformation.addRollingWindowFeatures(enriched)
      println("=== Aperçu transactions enrichies (montant cumulé 7j, utilisateur actif) ===")
      enrichedWithRolling.show(10, truncate = false)

      // ----- Partie 4 : Analytique business -----
      val analytics = new Analytics(sparkSession)

      val merchantReportDf = analytics.merchantReport(enriched)
      val cohortDf = analytics.cohortAnalysis(enriched)

      // `enriched` n'est plus nécessaire après ces deux calculs : on libère la mémoire.
      enriched.unpersist()

      // Partie 5.1 : chaque rapport est réutilisé pour l'affichage + 2 écritures (CSV/Parquet)
      merchantReportDf.cache()
      cohortDf.cache()

      println("=== Rapport détaillé par marchand ===")
      merchantReportDf.show(20, truncate = false)

      println("=== Analyse de cohortes utilisateurs ===")
      cohortDf.show(50, truncate = false)

      // ----- Partie 6 : Sauvegarde des résultats (CSV + Parquet) -----
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
