package com.esempio;

import java.io.Serializable;

/**
 * Modello di un Prodotto nel nostro negozio.
 * Deve implementare Serializable per essere usato con Spark.
 */
public class Prodotto implements Serializable {

    private int id;
    private String nome;
    private String categoria;
    private double prezzo;
    private int quantita;

    // Costruttore vuoto richiesto da Spark
    public Prodotto() {}

    public Prodotto(int id, String nome, String categoria, double prezzo, int quantita) {
        this.id = id;
        this.nome = nome;
        this.categoria = categoria;
        this.prezzo = prezzo;
        this.quantita = quantita;
    }

    // Getter e Setter (necessari per Spark Encoder)
    public int getId()               { return id; }
    public void setId(int id)        { this.id = id; }

    public String getNome()              { return nome; }
    public void setNome(String nome)     { this.nome = nome; }

    public String getCategoria()                 { return categoria; }
    public void setCategoria(String categoria)   { this.categoria = categoria; }

    public double getPrezzo()                { return prezzo; }
    public void setPrezzo(double prezzo)     { this.prezzo = prezzo; }

    public int getQuantita()                 { return quantita; }
    public void setQuantita(int quantita)    { this.quantita = quantita; }

    @Override
    public String toString() {
        return String.format(
            "Prodotto{id=%d, nome='%s', categoria='%s', prezzo=%.2f, quantita=%d}",
            id, nome, categoria, prezzo, quantita
        );
    }
}
