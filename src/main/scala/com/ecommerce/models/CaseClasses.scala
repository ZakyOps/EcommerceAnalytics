package com.ecommerce.models

/** Une transaction brute (transactions.csv). */
case class Transaction(
    transaction_id: String,
    user_id: String,
    product_id: String,
    merchant_id: String,
    amount: Double,
    timestamp: String, // format yyyyMMddHHmmss
    location: String,
    payment_method: String,
    category: String
)

/** Un utilisateur (users.json). */
case class User(
    user_id: String,
    age: Int,
    annual_income: Double,
    city: String,
    customer_segment: String,
    preferred_categories: Seq[String],
    registration_date: String // format yyyyMMdd
)

/** Un produit du catalogue (products.parquet). */
case class Product(
    product_id: String,
    name: String,
    category: String,
    price: Double,
    merchant_id: String,
    rating: Double,
    stock: Int
)

/** Un marchand (merchants.csv). */
case class Merchant(
    merchant_id: String,
    name: String,
    category: String,
    region: String,
    commission_rate: Double,
    establishment_date: String // format yyyyMMdd
)

/**
 * Caractéristiques temporelles extraites d'un timestamp de transaction par l'UDF
 * `extractTimeFeatures` (Partie 3.1). Spark infère automatiquement le schéma
 * struct correspondant à cette case class pour la colonne retournée par l'UDF.
 */
case class TimeFeatures(
    hour: Int,
    day_of_week: String,
    month: String,
    is_weekend: Int,
    day_period: String,
    is_working_hours: Int
)
