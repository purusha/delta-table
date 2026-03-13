package com.esempio;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Flow02 {

    public static void start(SparkSession spark) {

//        // Per versione numerica
//        Dataset<Row> version0 = spark.read()
//            .format("delta")
//            .option("versionAsOf", 0)
//            .load(Flow01.DELTA_PATH);
//
//        System.out.println("\n=== Version ZERO ===");
//        version0.show(); // dati originali, prima degli update!


        System.out.println("\n=== LOAD DeltaTable ===");
        DeltaTable deltaTable = DeltaTable.forPath(spark, Flow01.DELTA_PATH);
        deltaTable.history().show(false);

    }
}
