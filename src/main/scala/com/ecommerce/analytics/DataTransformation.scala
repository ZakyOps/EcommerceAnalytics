package com.ecommerce.analytics

import com.ecommerce.models._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

import java.time.{DayOfWeek, LocalDateTime}
import java.time.format.{DateTimeFormatter, TextStyle}
import java.util.Locale

/**
 * Partie 3.1 - logique de l'UDF extractTimeFeatures, isolée dans un objet
 * top-level (sans référence à SparkSession) pour que la closure envoyée aux
 * executors soit sérialisable : si la fonction avait été une méthode de la
 * classe DataTransformation, l'eta-expansion aurait capturé `this`, donc le
 * champ `sparkSession` (non sérialisable) -> "Task not serializable".
 */
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

  /** UDF publique réutilisable ; renvoie un struct correspondant à TimeFeatures. */
  val extractTimeFeatures: UserDefinedFunction = udf(extractTimeFeaturesLogic _)
}

/** Transformations avancées : UDF temporelle, jointures enrichies, fonctions de fenêtrage (Partie 3). */
class DataTransformation(sparkSession: SparkSession) {

  /** UDF publique réutilisable (Partie 3.1), déléguée à l'objet top-level TimeFeaturesUdf. */
  val extractTimeFeatures: UserDefinedFunction = TimeFeaturesUdf.extractTimeFeatures

  // ---------------------------------------------------------------------
  // Partie 3.2 - enrichTransactionData
  // ---------------------------------------------------------------------

  /**
   * Joint transactions/utilisateurs/produits/marchands, applique l'UDF temporelle
   * et ajoute les colonnes de fenêtrage (rang par utilisateur, nombre total de
   * transactions par utilisateur) ainsi que la tranche d'âge du client.
   */
  def enrichTransactionData(
      transactions: Dataset[Transaction],
      users: Dataset[User],
      products: Dataset[Product],
      merchants: Dataset[Merchant]
  ): DataFrame = {

    // On renomme les colonnes en collision (category, name, merchant_id) avant
    // la jointure pour garder un DataFrame final sans ambiguïté de colonnes.
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

    // Partie 5.2 - broadcast() de la petite table merchants pour éviter le shuffle
    // de la table de transactions (volumineuse) lors de la jointure.
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

  // ---------------------------------------------------------------------
  // Partie 3.3 - Analyse par partition Window (fenêtre glissante de 7 jours)
  // ---------------------------------------------------------------------

  /**
   * Ajoute, sur un DataFrame de transactions enrichi contenant `transaction_ts` :
   *  - rolling_7d_amount : somme des montants sur une fenêtre glissante de 7 jours
   *  - is_active_user_7d : 1 si l'utilisateur a transigé au moins 5 jours distincts
   *    sur cette même fenêtre glissante de 7 jours, 0 sinon
   */
  def addRollingWindowFeatures(df: DataFrame): DataFrame = {
    val sevenDaysInSeconds = 6L * 24 * 60 * 60 // jour courant + 6 jours précédents = 7 jours

    val rollingWindow = Window.partitionBy("user_id")
      .orderBy(col("transaction_ts").cast(LongType))
      .rangeBetween(-sevenDaysInSeconds, 0)

    df
      .withColumn("rolling_7d_amount", sum("amount").over(rollingWindow))
      .withColumn("active_days_7d", size(collect_set(to_date(col("transaction_ts"))).over(rollingWindow)))
      .withColumn("is_active_user_7d", when(col("active_days_7d") >= 5, 1).otherwise(0))
  }
}
