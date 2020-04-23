package com.github.mzovico.spark.file;

import com.github.mzovico.spark.udf.UUIDGenerator;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static java.lang.String.format;

public class ReadCassandraAndSaveOnFIle {

    private static final String KEY_SPACE = "credenciadora";
    private static final String TABLE = "credenciadora_table";
    public static final String CREDENCIADORA_VIEW = "credenciadora";


    public SparkSession createSparkSession(){

        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("testing")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        sparkSession.udf().register(UUIDGenerator.NAME, UUIDGenerator.UUID_GENERATOR, DataTypes.StringType);

        return sparkSession;
    }


    public StructField[] getFields() {
        return new StructField[]{
            new StructField("referencia_externa", DataTypes.StringType, true, Metadata.empty()),
            new StructField("data_referencia", DataTypes.DateType, true, Metadata.empty()),
            new StructField("cnpj_da_instituicao_credenciadora", DataTypes.StringType, true, Metadata.empty()),
            new StructField("data_de_liquidacao", DataTypes.DateType, true, Metadata.empty()),
            new StructField("valor_constituido_total", DataTypes.LongType, true, Metadata.empty()),
        };
    }

    public  Dataset<Row> readFile() {
        SparkSession sparkSession = createSparkSession();
        StructType schema = new StructType(getFields());


        // load file
        Dataset<Row> dataSet = sparkSession
                .read()
                .format("com.databricks.spark.csv")
                .option("header", true)
                .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                .option("delimiter", ";")
                .option("mode", "failFast")
                .option("quote", "\"")
                .schema(schema)
                .csv("/home/mgregorio/workspace/spark-basic/src/test/resources/credenciadora_file.csv");
//                .csv("/home/mgregorio/workspace/spark-basic/src/test/resources/credenciadora_file.csv.tar.gz");

        dataSet.printSchema();
        dataSet.show();

        return dataSet;

    }

    public Dataset<Row> insertColumn(Dataset<Row> dataset){
        // add column and get in another dataset
        Dataset<Row> anotherDataset = dataset.withColumn("idt_uuid", functions.callUDF(UUIDGenerator.NAME, functions.lit(1)))
                .withColumn("data_referencia_time_stamp", functions.to_timestamp(dataset.col("data_referencia")))
                .withColumn("data_redferencia_time_stamp_utc", functions.to_utc_timestamp(dataset.col("data_referencia"), "BRT"));
        anotherDataset.show();

        Dataset<Row> result = dataset.withColumn("idt_uuid", functions.callUDF(UUIDGenerator.NAME, functions.lit(1)));
        result.printSchema();
        result.show();

        return result;
    }

    public Dataset<Row> usingSqlFunctions(Dataset<Row> dataSet) {

        dataSet.createOrReplaceTempView(CREDENCIADORA_VIEW);

        String columns = "";

        for (String column : dataSet.columns()) {
            columns += format(", %s", column);
        }


        System.out.println(columns);
        String query = "SELECT uuid_gen(1) AS idt_uuid" + columns +" FROM " + CREDENCIADORA_VIEW;
        System.out.println(query);

        Dataset<Row> result = dataSet.sqlContext().sql(query);
        result.show();

        dataSet.sqlContext().sql(query).where("valor_constituido_total < 7000").show();
        dataSet.sqlContext().sql("SELECT * FROM " + CREDENCIADORA_VIEW + " where valor_constituido_total < 7000").show();

        return result;
    }


    public void insertOnCassandra(Dataset<Row> dataSet) {
        System.out.println("Saving on Cassandra");

        dataSet.write()
                .format("org.apache.spark.sql.cassandra")
                .mode(SaveMode.Append)
                .option("keyspace", KEY_SPACE)
                .option("table", TABLE)
                .option("spark.cassandra.connection.host", "localhost")
                .option("spark.cassandra.auth.username", "cassandra")
                .option("spark.cassandra.auth.password", "cassandra")
                .save();

        System.out.println("Done");
    }
}
