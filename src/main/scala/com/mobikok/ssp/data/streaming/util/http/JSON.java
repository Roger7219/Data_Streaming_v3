package com.mobikok.ssp.data.streaming.util.http;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.PackageVersion;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.mobikok.ssp.data.streaming.exception.JSONEntitySerializationException;
import com.mobikok.ssp.data.streaming.util.CSTTime;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class JSON extends Entity {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new SimpleModule  ("DateDeserializer", PackageVersion.VERSION)
            .addSerializer(Date.class, new JsonSerializer<Date>() {
                @Override
                public void serialize(Date value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {

                    SimpleDateFormat formatter = CSTTime.formatter("yyyy-MM-dd HH:mm:ss");//new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String formattedDate = formatter.format(value);
                    gen.writeString(formattedDate);
                }
            })
        )
        .setSerializationInclusion(Include.NON_NULL)
        .setDateFormat(CSTTime.formatter("yyyy-MM-dd HH:mm:ss") /*new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")*/);;

	protected JSON(){ }
	
	private Object bean;

	public JSON(Object bean) {
		this.bean = bean;
	}

	public String build() {
		try {
			return OBJECT_MAPPER.writeValueAsString(bean);
		} catch (JsonProcessingException e) {
			throw new JSONEntitySerializationException(e);
		}
	}
	
	public String toString() {
		return build();
	}
}
