# EcommerceAnalytics

Pipeline Spark/Scala d'analyse de données e-commerce : ingestion multi-format
(CSV, JSON, Parquet), validation, enrichissement (UDF + fonctions de fenêtrage),
analytique business (KPI marchands, cohortes utilisateurs) et sauvegarde des
résultats (CSV + Parquet).

## Structure du projet

```
EcommerceAnalytics/
├── build.sbt
├── project/build.properties
├── README.md
└── src/main/
    ├── scala/com/ecommerce/
    │   ├── models/CaseClasses.scala        # Transaction, User, Product, Merchant, TimeFeatures
    │   └── analytics/
    │       ├── DataIngestion.scala          # Partie 2 : lecture + validation + gestion d'erreurs
    │       ├── DataTransformation.scala      # Partie 3 : UDF, jointures, window functions
    │       ├── Analytics.scala               # Partie 4 : rapport marchands, cohortes
    │       └── MainApp.scala                 # Partie 5/6 : orchestration, cache, broadcast, IO
    └── resources/
        ├── application.conf                  # Partie 7 : configuration externalisée
        └── data/
            ├── transactions.csv
            ├── users.json
            ├── products.parquet
            └── merchants.csv
```

## Prérequis

- **JDK** 11 ou 17 (Spark 3.5.x)
- **Scala** 2.12.18 (téléchargé automatiquement par sbt via le plugin Scala)
- **sbt** 1.10+ (optionnel si `spark-submit`/`spark-shell` sont déjà installés séparément — sbt reste nécessaire pour compiler)
- **Apache Spark** 3.5.1 installé localement si vous voulez utiliser `spark-submit` en dehors de sbt

## Compilation

```bash
cd EcommerceAnalytics
sbt compile
```

## Générer le JAR exécutable

Deux options :

1. **JAR "fin" (thin jar)**, dépendances Spark fournies par le cluster/`spark-submit` :

   ```bash
   sbt package
   # target/scala-2.12/EcommerceAnalytics_2.12-0.1.jar
   ```

   Dans ce cas, il faut fournir la librairie Typesafe Config au lancement :

   ```bash
   spark-submit \
     --packages com.typesafe:config:1.4.3 \
     --class com.ecommerce.analytics.MainApp \
     --master local[*] \
     target/scala-2.12/EcommerceAnalytics_2.12-0.1.jar
   ```

2. **JAR "gras" (fat/uber jar)**, toutes les dépendances (dont Typesafe Config) embarquées :

   ```bash
   sbt assembly
   # target/scala-2.12/EcommerceAnalytics-assembly-0.1.jar
   ```

   ```bash
   spark-submit \
     --class com.ecommerce.analytics.MainApp \
     --master local[*] \
     target/scala-2.12/EcommerceAnalytics-assembly-0.1.jar
   ```

## Exécution locale (mode développement)

```bash
sbt run
```

Cela lance `MainApp` avec une `SparkSession` en `local[*]`, telle que configurée
dans `application.conf`.

## Exécution avec spark-submit (local)

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master local[*] \
  --driver-memory 2g \
  target/scala-2.12/EcommerceAnalytics-assembly-0.1.jar
```

## Déploiement sur un cluster (YARN)

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 5 \
  --executor-cores 4 \
  --executor-memory 8g \
  --conf spark.executor.memoryOverhead=1g \
  target/scala-2.12/EcommerceAnalytics-assembly-0.1.jar
```

## Configuration

Tous les chemins de données, le nom de l'application et les options Spark sont
externalisés dans `src/main/resources/application.conf` 
et lus via la librairie Typesafe Config (`com.typesafe.config.ConfigFactory`).

## Sorties

Le pipeline écrit les résultats finaux (rapport marchands et analyse de cohortes)
dans `output/` à la racine du projet, aux formats CSV et Parquet.
