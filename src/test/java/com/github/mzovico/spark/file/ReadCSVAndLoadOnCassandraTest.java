package com.github.mzovico.spark.file;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

public class ReadCSVAndLoadOnCassandraTest {


    private ReadCSVAndLoadOnCassandra readCSVAndLoadOnCassandra;

    @Before
    public void setUp() {
        readCSVAndLoadOnCassandra = new ReadCSVAndLoadOnCassandra();
    }

    @Test
    public void shouldSaveOnCassandra() {

        Dataset<Row> dataset = readCSVAndLoadOnCassandra.readFile();

        Dataset<Row> result = readCSVAndLoadOnCassandra.insertColumn(dataset);

        readCSVAndLoadOnCassandra.insertOnCassandra(result);
    }


    @Test
    public void shouldSaveOnCassandraWithSqlFunctions() {

        Dataset<Row> dataset = readCSVAndLoadOnCassandra.readFile();

        Dataset<Row> result = readCSVAndLoadOnCassandra.usingSqlFunctions(dataset);

        readCSVAndLoadOnCassandra.insertOnCassandra(result);
    }
}
