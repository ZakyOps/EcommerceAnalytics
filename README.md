# EcommerceAnalytics

Pipeline Spark/Scala : ingestion multi-format (CSV, JSON, Parquet), validation,
enrichissement (UDF + window functions), analytique business (KPI marchands,
cohortes utilisateurs), sauvegarde des résultats en CSV + Parquet.

## Structure

```
EcommerceAnalytics/
├── build.sbt
├── project/build.properties
└── src/main/
    ├── scala/com/ecommerce/
    │   ├── models/CaseClasses.scala
    │   └── analytics/
    │       ├── DataIngestion.scala
    │       ├── DataTransformation.scala
    │       ├── Analytics.scala
    │       └── MainApp.scala
    └── resources/
        ├── application.conf
        └── data/
```

## Prérequis

JDK 11/17, sbt 1.10+. Scala et Spark sont téléchargés automatiquement par sbt.

## Compiler / lancer

```bash
sbt compile
sbt run
```

## Générer le jar exécutable

```bash
sbt assembly
spark-submit --class com.ecommerce.analytics.MainApp --master local[*] \
  target/scala-2.12/EcommerceAnalytics-assembly-0.1.jar
```

(`sbt package` fabrique un jar plus léger sans les dépendances ; dans ce cas
il faut ajouter `--packages com.typesafe:config:1.4.3` au spark-submit.)

## Déploiement cluster (YARN)

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master yarn --deploy-mode cluster \
  --num-executors 5 --executor-cores 4 --executor-memory 8g \
  --conf spark.executor.memoryOverhead=1g \
  target/scala-2.12/EcommerceAnalytics-assembly-0.1.jar
```

## Configuration

Chemins de données et options Spark externalisés dans
`src/main/resources/application.conf` (Typesafe Config).

## Sorties

Résultats écrits dans `output/` (CSV + Parquet) : rapport marchands et
analyse de cohortes.
