package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

/**
 * Centralise la lecture des 4 sources de données du projet (Partie 2.1),
 * leur validation (Partie 2.2) et la gestion des erreurs de chargement (Partie 2.3).
 */
class DataIngestion(sparkSession: SparkSession) {

  import sparkSession.implicits._

  /** transactions.csv : schéma défini explicitement (Partie 2.1). */
  private val transactionSchema = StructType(Seq(
    StructField("transaction_id", StringType, nullable = false),
    StructField("user_id", StringType, nullable = false),
    StructField("product_id", StringType, nullable = false),
    StructField("merchant_id", StringType, nullable = false),
    StructField("amount", DoubleType, nullable = false),
    StructField("timestamp", StringType, nullable = false),
    StructField("location", StringType, nullable = true),
    StructField("payment_method", StringType, nullable = true),
    StructField("category", StringType, nullable = true)
  ))

  // ---------------------------------------------------------------------
  // Partie 2.2 - Validation : une fonction de règles par dataset
  // ---------------------------------------------------------------------

  def validateTransactions(ds: Dataset[Transaction]): Dataset[Transaction] =
    ds.filter(t => t.amount > 0 && t.timestamp != null && t.timestamp.length == 14)

  def validateUsers(ds: Dataset[User]): Dataset[User] =
    ds.filter(u => u.age >= 16 && u.age <= 100 && u.annual_income > 0)

  def validateProducts(ds: Dataset[Product]): Dataset[Product] =
    ds.filter(p => p.price > 0 && p.rating >= 1 && p.rating <= 5)

  def validateMerchants(ds: Dataset[Merchant]): Dataset[Merchant] =
    ds.filter(m => m.commission_rate >= 0 && m.commission_rate <= 1)

  // ---------------------------------------------------------------------
  // Partie 2.1 + 2.3 - Lecture, try/catch, bilan lignes lues / lignes valides
  // ---------------------------------------------------------------------

  def loadTransactions(path: String): Dataset[Transaction] =
    loadAndValidate("transactions.csv", sparkSession.emptyDataset[Transaction]) {
      val raw = sparkSession.read
        .schema(transactionSchema)
        .option("header", "true")
        .csv(path)
        .as[Transaction]
      (raw, validateTransactions(raw))
    }

  def loadUsers(path: String): Dataset[User] =
    loadAndValidate("users.json", sparkSession.emptyDataset[User]) {
      // users.json contient un champ imbriqué (preferred_categories: Array[String]) ;
      // spark.read.json infère nativement les tableaux/structures JSON. Les entiers
      // JSON sans décimale sont inférés en BIGINT (Long) : on re-caste "age" en Int
      // pour correspondre au type de la case class User (Long -> Int est un narrowing
      // cast que Spark refuse implicitement lors du .as[User]).
      val raw = sparkSession.read
        .option("multiline", "false")
        .json(path)
        .withColumn("age", col("age").cast(IntegerType))
        .as[User]
      (raw, validateUsers(raw))
    }

  def loadProducts(path: String): Dataset[Product] =
    loadAndValidate("products.parquet", sparkSession.emptyDataset[Product]) {
      val raw = sparkSession.read.parquet(path).as[Product]
      (raw, validateProducts(raw))
    }

  def loadMerchants(path: String): Dataset[Merchant] =
    loadAndValidate("merchants.csv", sparkSession.emptyDataset[Merchant]) {
      // Schéma laissé à l'inférence de Spark (Partie 2.1). establishment_date
      // ressemble à un nombre (ex: 20220918) : Spark l'infère en Int, on la
      // re-caste donc en String pour respecter le type documenté du dataset.
      val raw = sparkSession.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .withColumn("establishment_date", col("establishment_date").cast(StringType))
        .as[Merchant]
      (raw, validateMerchants(raw))
    }

  /**
   * Factorise le bloc try/catch commun aux 4 lectures : capture les erreurs de
   * lecture (fichier introuvable, structure incorrecte, etc.), puis affiche le
   * nombre de lignes lues avant validation et le nombre de lignes valides après.
   */
  private def loadAndValidate[T](
      label: String,
      onError: => Dataset[T]
  )(readAndValidate: => (Dataset[T], Dataset[T])): Dataset[T] = {
    try {
      val (raw, valid) = readAndValidate
      val before = raw.count()
      val after = valid.count()
      println(s"[DataIngestion] $label : $before ligne(s) lue(s) avant validation, " +
        s"$after ligne(s) valide(s) après validation (${before - after} rejetée(s))")
      valid
    } catch {
      case e: Exception =>
        println(s"[DataIngestion] ERREUR lors du chargement de $label : ${e.getMessage}")
        onError
    }
  }
}
