package com.esempio;

import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class NegozioDemo {

    public static void main(String[] args) {
        System.out.println("\nargs = " + Arrays.toString(args));

        // ============================================================
        // STEP 1: Creare la SparkSession
        // ============================================================
        // È il "punto di ingresso" a tutto Spark.
        // Con local[*] usiamo tutti i core della macchina locale.
        // Le due config sono OBBLIGATORIE per abilitare Delta Lake.
        // ============================================================
        System.out.println("\n=== STEP 1: Creo la SparkSession ===");

        SparkSession spark = SparkSession.builder()
            .appName("NegozioDemo")
            .master("local[*]")
            // Abilita la sintassi SQL di Delta Lake
            .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
            // Registra il catalogo Delta (necessario per usare DeltaTable.forPath ecc.)
            .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            // Riduce i log di Spark per rendere l'output più leggibile
            .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=ERROR")
            .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        System.out.println("SparkSession creata con successo!");


        for (String flow : args) {
            if ("01".equals(flow) || "1".equals(flow)) {
                Flow01.start(spark);
            }
            if ("02".equals(flow) || "2".equals(flow)) {
                Flow02.start(spark);
            }
        }
    }
}