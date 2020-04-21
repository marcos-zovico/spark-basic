package com.github.mzovico.spark.udf;

import static junit.framework.TestCase.assertEquals;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NormalizedASCIITest {

    private SparkSession spark;

    @Before
    public void setUp() {
        spark = SparkSession.builder()
                .master("local[*]")
                .appName("testing")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
        
        spark.udf().register(NormalizedASCII.NAME, NormalizedASCII.NORMALIZED_ASCII, DataTypes.StringType);
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    @Test
    public void shouldClearAnySpecialCharacter() {
        Row result = spark.sql("SELECT normalized_ascii('áâãàçéêíóôõú', 'description')").head();
        System.out.println(result.getString(0));
        assertEquals("aaaaceeiooou", result.getString(0));
    }
}