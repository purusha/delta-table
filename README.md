# Delta Lake Demo — Java

Esempio completo di utilizzo di **Delta Lake con Java e Apache Spark**.

## Scenario
Gestione del catalogo prodotti di un negozio. La tabella contiene:
- `id`, `nome`, `categoria`, `prezzo`, `quantita`

## Operazioni mostrate

| Step | Operazione | Descrizione |
|------|-----------|-------------|
| 1 | SparkSession | Configurazione del punto di ingresso |
| 2 | CREATE / WRITE | Scrittura dati iniziali come Delta Table |
| 3 | READ | Lettura della tabella |
| 4 | SQL | Query con filtri e aggregazioni |
| 5 | UPDATE | Aumento prezzi Elettronica del 10% |
| 6 | DELETE | Rimozione prodotti esauriti |
| 7 | MERGE (Upsert) | Sincronizzazione con nuovo listino |
| 8 | TIME TRAVEL | Lettura versione precedente |
| 9 | HISTORY | Storia di tutte le modifiche |
| 10 | VACUUM | Pulizia file obsoleti |

## Prerequisiti

- Java 17+
- Maven 3.6+

## Come eseguire

```bash
# 1. Compila il progetto
mvn clean package -q

# 2. Esegui il demo
java -jar target/delta-lake-demo-1.0-SNAPSHOT.jar
```

## Struttura del progetto

```
delta-lake-demo/
├── pom.xml
└── src/main/java/com/esempio/
    ├── Prodotto.java      ← Modello dati (JavaBean)
    └── NegozioDemo.java   ← Demo principale con tutti gli esempi
```

## Output della Delta Table

I file vengono salvati in `/tmp/negozio-prodotti/` con questa struttura:

```
/tmp/negozio-prodotti/
├── _delta_log/          ← transaction log (JSON di ogni operazione)
│   ├── 00000000000000000000.json   ← versione 0: CREATE
│   ├── 00000000000000000001.json   ← versione 1: UPDATE
│   ├── 00000000000000000002.json   ← versione 2: APPEND (prodotto esaurito)
│   ├── 00000000000000000003.json   ← versione 3: DELETE
│   └── 00000000000000000004.json   ← versione 4: MERGE
└── part-*.parquet       ← dati effettivi in formato Parquet
```

## Dipendenze principali

```xml
<!-- Apache Spark -->
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.12</artifactId>
    <version>3.5.0</version>
</dependency>

<!-- Delta Lake -->
<dependency>
    <groupId>io.delta</groupId>
    <artifactId>delta-spark_2.12</artifactId>
    <version>3.2.0</version>
</dependency>
```
