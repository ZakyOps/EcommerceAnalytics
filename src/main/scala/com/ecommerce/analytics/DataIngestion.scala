package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class DataIngestion(sparkSession: SparkSession) {

  import sparkSession.implicits._

  // schéma défini explicitement pour transactions.csv
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

  def validateTransactions(ds: Dataset[Transaction]): Dataset[Transaction] =
    ds.filter(t => t.amount > 0 && t.timestamp != null && t.timestamp.length == 14)

  def validateUsers(ds: Dataset[User]): Dataset[User] =
    ds.filter(u => u.age >= 16 && u.age <= 100 && u.annual_income > 0)

  def validateProducts(ds: Dataset[Product]): Dataset[Product] =
    ds.filter(p => p.price > 0 && p.rating >= 1 && p.rating <= 5)

  def validateMerchants(ds: Dataset[Merchant]): Dataset[Merchant] =
    ds.filter(m => m.commission_rate >= 0 && m.commission_rate <= 1)

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
      // age est inféré en Long (BIGINT) par spark.read.json, on le recaste en Int
      // pour matcher la case class User
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
      // inferSchema laisse Spark deviner les types, mais establishment_date
      // (ex: 20220918) est alors inféré en Int -> on la recaste en String
      val raw = sparkSession.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .withColumn("establishment_date", col("establishment_date").cast(StringType))
        .as[Merchant]
      (raw, validateMerchants(raw))
    }

  // factorise le try/catch commun aux 4 lectures + le bilan lignes lues/valides
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
