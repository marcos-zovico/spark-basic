package com.github.mzovico.spark.udf;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import static com.github.mzovico.spark.udf.StringToDate.STRING_TO_DATE;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

public class StringToDateTest {

    private transient SparkSession spark;

    @Before
    public void setUp() {
        spark = SparkSession.builder()
                .master("local[*]")
                .appName("testing")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();

        spark.udf().register(StringToDate.NAME, STRING_TO_DATE, DataTypes.DateType);
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    @Test
    public void shouldParseStringToDate() {
        Row result = spark.sql("SELECT string_to_date('22/04/2020', 'dd/MM/yyyy', 'created_date')").head();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        assertEquals("2020-04-22", df.format(result.getDate(0)));
        assertEquals(Date.class, result.getDate(0).getClass());
    }


    @Test
    public void shouldNotParseEmptyStringToDate() {
        try {
            spark.sql("SELECT string_to_date('', 'yyyy-MM-dd', 'created_date')").head();
            fail();
        } catch (Exception e) {
            assertEquals(DateParserException.class, ExceptionUtils.getRootCause(e).getClass());
        }
    }
}