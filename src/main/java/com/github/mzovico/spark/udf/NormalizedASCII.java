package com.github.mzovico.spark.udf;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF2;

import java.io.Serializable;
import java.text.Normalizer;

public class NormalizedASCII implements Serializable {

    /**
     * UDF name
     */
	public static final String NAME = "normalized_ascii";

	/**
     * Remove qualquer caracter com acentuaçao;
     * <pre>
     *     áâãàçéêíóôõú --> aaaaceeiooou
     * <pre/>
     */
    public static final  UDF2<String, String, String> NORMALIZED_ASCII = (value, fieldName) -> {
        try {
            if (StringUtils.isBlank(value)) {
                return "";
            } else {
                String unescapedValue = StringEscapeUtils.unescapeCsv(value);
                return Normalizer.normalize(unescapedValue, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
            }
        } catch (Exception e) {
            throw new TextParserException(String.format("Não foi possível mapear a coluna texto '%s', com o valor '%s'", fieldName, value));
        }
    };
}
