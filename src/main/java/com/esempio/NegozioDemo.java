package com.esempio;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * ============================================================
 *  DEMO COMPLETO: Delta Lake con Java
 *  Scenario: gestione catalogo prodotti di un negozio
 * ============================================================
 *
 * Operazioni mostrate:
 *  1. Creazione SparkSession
 *  2. Creazione e scrittura Delta Table
 *  3. Lettura Delta Table
 *  4. Query SQL sulla tabella
 *  5. UPDATE di alcuni record
 *  6. DELETE di record
 *  7. MERGE / Upsert (la più potente!)
 *  8. Time Travel (leggere versioni precedenti)
 *  9. Storia delle modifiche
 * 10. VACUUM (pulizia file obsoleti)
 */
public class NegozioDemo {

    // Percorso dove salveremo la Delta Table (in locale per il demo)
    private static final String DELTA_PATH = "/tmp/negozio-prodotti";

    public static void main(String[] args) {

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


        // ============================================================
        // STEP 2: Creare dati iniziali e scriverli come Delta Table
        // ============================================================
        // Usiamo Encoders.bean() per dire a Spark come serializzare
        // la nostra classe Java Prodotto in un Dataset tipizzato.
        // ============================================================
        System.out.println("\n=== STEP 2: Creo e scrivo la Delta Table ===");

        List<Prodotto> prodottiIniziali = Arrays.asList(
            new Prodotto(1, "MacBook Pro",      "Elettronica",   1999.99, 15),
            new Prodotto(2, "Mouse Logitech",   "Elettronica",     49.99, 80),
            new Prodotto(3, "Scrivania IKEA",   "Arredamento",    349.00, 10),
            new Prodotto(4, "Sedia Ergonomica", "Arredamento",    299.00, 25),
            new Prodotto(5, "Notebook A4",      "Cancelleria",      3.50, 500),
            new Prodotto(6, "Penna BIC",        "Cancelleria",      0.80, 1000),
            new Prodotto(7, "Monitor LG 27\"",  "Elettronica",    399.00, 30),
            new Prodotto(8, "Tastiera Meccanica","Elettronica",   129.99, 45)
        );

        // Creo un Dataset<Prodotto> (tabella tipizzata Spark)
        Dataset<Prodotto> dsProdotti = spark.createDataset(
            prodottiIniziali,
            Encoders.bean(Prodotto.class)
        );

        // Scrivo come Delta Table sul filesystem
        // mode("overwrite") sovrascrive se esiste già (utile per rieseguire il demo)
        dsProdotti.write()
            .format("delta")
            .mode("overwrite")
            .save(DELTA_PATH);

        System.out.println("Delta Table creata in: " + DELTA_PATH);


        // ============================================================
        // STEP 3: Leggere la Delta Table
        // ============================================================
        System.out.println("\n=== STEP 3: Leggo la Delta Table ===");

        Dataset<Row> tabella = spark.read()
            .format("delta")
            .load(DELTA_PATH);

        System.out.println("Tutti i prodotti nel catalogo:");
        tabella.orderBy("id").show();


        // ============================================================
        // STEP 4: Query SQL
        // ============================================================
        // Registriamo la tabella come "vista temporanea" per usare SQL
        // ============================================================
        System.out.println("\n=== STEP 4: Query SQL ===");

        tabella.createOrReplaceTempView("prodotti");

        System.out.println("Prodotti categoria Elettronica con prezzo > 100:");
        spark.sql("""
                SELECT nome, prezzo, quantita
                FROM prodotti
                WHERE categoria = 'Elettronica' AND prezzo > 100
                ORDER BY prezzo DESC
                """).show();

        System.out.println("Totale valore in magazzino per categoria:");
        spark.sql("""
                SELECT
                    categoria,
                    COUNT(*)               AS num_prodotti,
                    ROUND(SUM(prezzo * quantita), 2) AS valore_totale
                FROM prodotti
                GROUP BY categoria
                ORDER BY valore_totale DESC
                """).show();


        // ============================================================
        // STEP 5: UPDATE — modifica prezzi Elettronica (+10%)
        // ============================================================
        System.out.println("\n=== STEP 5: UPDATE prezzi Elettronica +10% ===");

        DeltaTable deltaTable = DeltaTable.forPath(spark, DELTA_PATH);

        Map<String, Column> aggiornamenti = new HashMap<>();
        aggiornamenti.put("prezzo", col("prezzo").multiply(1.10));

        deltaTable.update(
            col("categoria").equalTo("Elettronica"),  // condizione WHERE
            aggiornamenti                              // SET
        );

        System.out.println("Prezzi aggiornati. Nuovi prezzi Elettronica:");
        spark.read().format("delta").load(DELTA_PATH)
            .filter(col("categoria").equalTo("Elettronica"))
            .select("nome", "prezzo")
            .orderBy("nome")
            .show();


        // ============================================================
        // STEP 6: DELETE — rimuovi prodotti esauriti (quantita = 0)
        // (Prima ne aggiungiamo uno esaurito per mostrare la funzione)
        // ============================================================
        System.out.println("\n=== STEP 6: DELETE prodotti esauriti ===");

        // Aggiungo un prodotto esaurito
        List<Prodotto> esauriti = Arrays.asList(
            new Prodotto(9, "Webcam HD", "Elettronica", 89.99, 0)
        );
        spark.createDataset(esauriti, Encoders.bean(Prodotto.class))
            .write().format("delta").mode("append").save(DELTA_PATH);

        System.out.println("Prima del DELETE (prodotto con quantita=0):");
        spark.read().format("delta").load(DELTA_PATH)
            .filter(col("quantita").equalTo(0))
            .show();

        // Cancello tutti i prodotti esauriti
        deltaTable.delete(col("quantita").equalTo(0));

        System.out.println("Dopo il DELETE — nessun prodotto esaurito rimasto:");
        spark.read().format("delta").load(DELTA_PATH)
            .filter(col("quantita").equalTo(0))
            .show(); // → tabella vuota, come previsto


        // ============================================================
        // STEP 7: MERGE (Upsert) — sincronizza con nuovo listino prezzi
        // ============================================================
        // MERGE è l'operazione più potente: riceve dati "nuovi" e:
        //  - Se il prodotto ESISTE già → lo AGGIORNA
        //  - Se il prodotto NON ESISTE  → lo INSERISCE
        //  - (opzionale) Se NON è nei nuovi dati → lo CANCELLA
        // ============================================================
        System.out.println("\n=== STEP 7: MERGE (Upsert) con nuovo listino ===");

        List<Prodotto> nuovoListino = Arrays.asList(
            // Aggiornamento: Mouse costa di più
            new Prodotto(2, "Mouse Logitech",   "Elettronica",  59.99, 80),
            // Aggiornamento: più scorte per le sedie
            new Prodotto(4, "Sedia Ergonomica", "Arredamento", 299.00, 50),
            // Nuovo prodotto: non esisteva prima
            new Prodotto(10, "Webcam 4K Pro",   "Elettronica", 149.99, 20)
        );

        Dataset<Row> dsNuovoListino = spark.createDataset(
            nuovoListino, Encoders.bean(Prodotto.class)
        ).toDF();

        // In Java l'API MERGE usa una catena a due livelli:
        //   .whenMatched()   → restituisce DeltaMergeMatchedActionBuilder
        //   .updateExpr(...) → specifica cosa aggiornare (colonna → espressione SQL)
        //   .whenNotMatched()   → restituisce DeltaMergeNotMatchedActionBuilder
        //   .insertExpr(...)    → specifica cosa inserire
        deltaTable.as("vecchio")
            .merge(dsNuovoListino.as("nuovo"), "vecchio.id = nuovo.id")
            // Se l'id esiste in entrambi → aggiorna tutti i campi con i valori del nuovo listino
            .whenMatched()
            .updateExpr(new HashMap<String, String>() {{
                put("nome",      "nuovo.nome");
                put("categoria", "nuovo.categoria");
                put("prezzo",    "nuovo.prezzo");
                put("quantita",  "nuovo.quantita");
            }})
            // Se l'id esiste solo nel nuovo listino → inserisci come nuovo prodotto
            .whenNotMatched()
            .insertExpr(new HashMap<String, String>() {{
                put("id",        "nuovo.id");
                put("nome",      "nuovo.nome");
                put("categoria", "nuovo.categoria");
                put("prezzo",    "nuovo.prezzo");
                put("quantita",  "nuovo.quantita");
            }})
            .execute();

        System.out.println("Dopo il MERGE:");
        spark.read().format("delta").load(DELTA_PATH)
            .orderBy("id")
            .show();


        // ============================================================
        // STEP 8: TIME TRAVEL — torna alla versione 0 (dati originali)
        // ============================================================
        System.out.println("\n=== STEP 8: Time Travel — versione originale (v0) ===");

        Dataset<Row> versioneOriginale = spark.read()
            .format("delta")
            .option("versionAsOf", 0)   // ← legge la versione 0!
            .load(DELTA_PATH);

        System.out.println("Dati come erano all'inizio (versione 0):");
        versioneOriginale.orderBy("id").show();

        System.out.println("(confronto) Dati attuali:");
        spark.read().format("delta").load(DELTA_PATH).orderBy("id").show();


        // ============================================================
        // STEP 9: HISTORY — vedi tutte le modifiche fatte
        // ============================================================
        System.out.println("\n=== STEP 9: Storia completa delle modifiche ===");

        deltaTable.history()
            .select("version", "timestamp", "operation", "operationParameters")
            .show(20, false);


        // ============================================================
        // STEP 10: VACUUM — pulisce file obsoleti (> 7 giorni default)
        // ============================================================
        // (In questo demo usiamo retentionHours=0 solo per mostrare
        //  come si chiama — in produzione usare il default di 168h!)
        // ============================================================
        System.out.println("\n=== STEP 10: VACUUM (pulizia file vecchi) ===");

        // Disabilita il controllo retention solo per il demo
        spark.conf().set("spark.databricks.delta.retentionDurationCheck.enabled", "false");

        deltaTable.vacuum(0); // 0 ore = rimuove SUBITO i file non più necessari
        // In produzione: deltaTable.vacuum(); // usa il default di 7 giorni

        System.out.println("VACUUM completato!");


        // ============================================================
        // Fine
        // ============================================================
        System.out.println("\n✅ Demo completato con successo!");
        System.out.println("   Operazioni eseguite: CREATE, READ, SQL, UPDATE, DELETE, MERGE, TIME TRAVEL, HISTORY, VACUUM");

        spark.stop();
    }
}