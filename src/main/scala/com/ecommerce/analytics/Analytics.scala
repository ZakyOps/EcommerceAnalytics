package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class Analytics(sparkSession: SparkSession) {

  // enriched = sortie de DataTransformation.enrichTransactionData
  def merchantReport(enriched: DataFrame): DataFrame = {
    val baseAgg = enriched
      .groupBy("merchant_id", "merchant_name", "merchant_category", "region")
      .agg(
        round(sum("amount"), 2).as("total_revenue"),
        count(lit(1)).as("nb_transactions"),
        countDistinct("user_id").as("unique_clients"),
        round(avg("amount"), 2).as("avg_transaction_amount"),
        round(sum(col("amount") * col("commission_rate")), 2).as("total_commission")
      )

    // Classement par CA dans sa catégorie et sa région (Window functions)
    val categoryRankWindow = Window.partitionBy("merchant_category").orderBy(desc("total_revenue"))
    val regionRankWindow = Window.partitionBy("region").orderBy(desc("total_revenue"))

    val ranked = baseAgg
      .withColumn("rank_in_category", rank().over(categoryRankWindow))
      .withColumn("rank_in_region", rank().over(regionRankWindow))

    // Répartition des ventes par tranche d'âge des clients (pivot)
    val ageDistribution = enriched
      .groupBy("merchant_id")
      .pivot("age_group", Seq("Jeune", "Adulte", "Age Moyen", "Senior"))
      .agg(round(sum("amount"), 2))
      .na.fill(0.0)
      .toDF("merchant_id", "revenue_jeune", "revenue_adulte", "revenue_age_moyen", "revenue_senior")

    ranked
      .join(ageDistribution, Seq("merchant_id"), "left")
      .orderBy(desc("total_revenue"))
  }

  // regroupe les users par mois de 1ère transaction (cohort_month), puis compte
  // les users actifs distincts par période (period_number = mois écoulés depuis la cohorte)
  def cohortAnalysis(enriched: DataFrame): DataFrame = {
    val userFirstTransaction = enriched
      .groupBy("user_id")
      .agg(min("transaction_ts").as("first_transaction_ts"))
      .withColumn("cohort_month", date_format(col("first_transaction_ts"), "yyyy-MM"))

    val withCohort = enriched
      .join(userFirstTransaction, Seq("user_id"), "inner")
      .withColumn("activity_month", date_format(col("transaction_ts"), "yyyy-MM"))
      .withColumn(
        "period_number",
        months_between(
          to_date(col("activity_month"), "yyyy-MM"),
          to_date(col("cohort_month"), "yyyy-MM")
        ).cast("int")
      )

    withCohort
      .groupBy("cohort_month", "period_number")
      .agg(countDistinct("user_id").as("active_users"))
      .orderBy("cohort_month", "period_number")
  }
}
