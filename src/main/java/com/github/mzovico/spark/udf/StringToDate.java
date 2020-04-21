package com.github.mzovico.spark.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF3;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;


import java.io.Serializable;
import java.sql.Date;

public class StringToDate implements Serializable {

    /**
     * UDF name
     */
    public static final String NAME = "string_to_date";

    /**
     * Converte um data string para data
     */
    public static final UDF3 STRING_TO_DATE = (UDF3<String, String, String, Date>) (dateIn, format, fieldName) -> {

        if (StringUtils.isBlank(dateIn)) {
            throw new DateParserException(String.format("A coluna de data '%s' contém valores vazios.", fieldName));
        }

        try {
            DateTimeParser[] parsers = {
                    DateTimeFormat.forPattern(format + " HH:mm:ss").getParser(),
                    DateTimeFormat.forPattern(format + " HH:mm").getParser(),
                    DateTimeFormat.forPattern(format + " HH").getParser(),
                    DateTimeFormat.forPattern(format).getParser()};

            DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(null, parsers).toFormatter();
            LocalDateTime dateTime = formatter.parseLocalDateTime(dateIn.trim());
            return new Date(dateTime.toDate().getTime());
        } catch (Exception e) {
            throw new DateParserException(String.format("Não foi possível mapear a coluna data '%s', com o valor '%s' utilizando o formato '%s'", fieldName, dateIn, format));
        }
    };
}
