package com.github.mzovico.spark.udf;

import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;

import java.io.Serializable;
import java.util.UUID;

public class UUIDGenerator implements Serializable {

    /**
     * UDF name
     */
	public static final String NAME = "uuid_gen";


    public static final UDF1<Integer, String> UUID_GENERATOR = (value) -> UUID.randomUUID().toString();
}
