package com.ecommerce.analytics

import com.ecommerce.models._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

import java.time.{DayOfWeek, LocalDateTime}
import java.time.format.{DateTimeFormatter, TextStyle}
import java.util.Locale

// logique de l'UDF isolée ici (objet top-level, pas de référence à SparkSession)
// sinon l'eta-expansion capture `this` -> Task not serializable
object TimeFeaturesUdf {

  private val timestampFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  def extractTimeFeaturesLogic(ts: String): TimeFeatures = {
    val dt = LocalDateTime.parse(ts, timestampFormatter)
    val hour = dt.getHour
    val dayOfWeek = dt.getDayOfWeek.getDisplayName(TextStyle.FULL, Locale.ENGLISH)
    val month = dt.getMonth.getDisplayName(TextStyle.FULL, Locale.ENGLISH)
    val isWeekend = if (dt.getDayOfWeek == DayOfWeek.SATURDAY || dt.getDayOfWeek == DayOfWeek.SUNDAY) 1 else 0
    val dayPeriod =
      if (hour < 6) "Night"
      else if (hour < 12) "Morning"
      else if (hour < 18) "Afternoon"
      else if (hour < 22) "Evening"
      else "Night"
    val isWorkingHours = if (hour >= 9 && hour <= 17) 1 else 0
    TimeFeatures(hour, dayOfWeek, month, isWeekend, dayPeriod, isWorkingHours)
  }

  val extractTimeFeatures: UserDefinedFunction = udf(extractTimeFeaturesLogic _)
}

class DataTransformation(sparkSession: SparkSession) {

  val extractTimeFeatures: UserDefinedFunction = TimeFeaturesUdf.extractTimeFeatures

  def enrichTransactionData(
      transactions: Dataset[Transaction],
      users: Dataset[User],
      products: Dataset[Product],
      merchants: Dataset[Merchant]
  ): DataFrame = {

    // category/name/merchant_id existent dans plusieurs tables -> on renomme avant de joindre
    val txn = transactions.toDF().withColumnRenamed("category", "txn_category")

    val prod = products.toDF()
      .select(
        col("product_id"),
        col("name").as("product_name"),
        col("category").as("product_category"),
        col("price").as("product_price"),
        col("rating"),
        col("stock")
      )

    val merch = merchants.toDF()
      .select(
        col("merchant_id"),
        col("name").as("merchant_name"),
        col("category").as("merchant_category"),
        col("region"),
        col("commission_rate"),
        col("establishment_date")
      )

    // broadcast sur merchants (petite table, 500 lignes) pour éviter un shuffle
    // de la table de transactions
    val joined = txn
      .join(users.toDF(), Seq("user_id"), "inner")
      .join(prod, Seq("product_id"), "inner")
      .join(broadcast(merch), Seq("merchant_id"), "inner")

    val withTimeFeatures = joined
      .withColumn("transaction_ts", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
      .withColumn("time_features", extractTimeFeatures(col("timestamp")))
      .withColumn("hour", col("time_features.hour"))
      .withColumn("day_of_week", col("time_features.day_of_week"))
      .withColumn("month", col("time_features.month"))
      .withColumn("is_weekend", col("time_features.is_weekend"))
      .withColumn("day_period", col("time_features.day_period"))
      .withColumn("is_working_hours", col("time_features.is_working_hours"))
      .drop("time_features")

    val userOrderedWindow = Window.partitionBy("user_id").orderBy("transaction_ts")
    val userWindow = Window.partitionBy("user_id")

    withTimeFeatures
      .withColumn("transaction_rank_for_user", row_number().over(userOrderedWindow))
      .withColumn("total_transactions_user", count(lit(1)).over(userWindow))
      .withColumn(
        "age_group",
        when(col("age") < 25, "Jeune")
          .when(col("age") <= 44, "Adulte")
          .when(col("age") <= 64, "Age Moyen")
          .otherwise("Senior")
      )
  }

  // fenêtre glissante de 7 jours : montant cumulé + détection utilisateur actif
  def addRollingWindowFeatures(df: DataFrame): DataFrame = {
    val sevenDaysInSeconds = 6L * 24 * 60 * 60 // jour courant + 6 précédents

    val rollingWindow = Window.partitionBy("user_id")
      .orderBy(col("transaction_ts").cast(LongType))
      .rangeBetween(-sevenDaysInSeconds, 0)

    df
      .withColumn("rolling_7d_amount", sum("amount").over(rollingWindow))
      .withColumn("active_days_7d", size(collect_set(to_date(col("transaction_ts"))).over(rollingWindow)))
      .withColumn("is_active_user_7d", when(col("active_days_7d") >= 5, 1).otherwise(0))
  }
}
