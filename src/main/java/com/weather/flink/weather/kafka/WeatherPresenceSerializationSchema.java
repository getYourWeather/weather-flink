package com.weather.flink.weather.kafka;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.weather.info.avro.WeatherData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WeatherPresenceSerializationSchema implements KafkaRecordSerializationSchema<WeatherData> {
    public static final ObjectMapper mapper = new ObjectMapper();
    private String topic;
    private static final Logger logger = LoggerFactory.getLogger(WeatherPresenceSerializationSchema.class);
    public WeatherPresenceSerializationSchema(String topic) {
        this.topic = topic;
    }

    static {
        SimpleModule simpleModule = new SimpleModule("SimpleModule");
        simpleModule.addSerializer(CharSequence.class, new JsonSerializer<CharSequence>() {
            @Override
            public void serialize(CharSequence charSequence, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
                jsonGenerator.writeString(charSequence.toString());
            }
        });
        mapper.registerModule(simpleModule);

        mapper
                // .addMixIn(CharSequence.class, CharSeqSerializer.class)
                .addMixIn(GenericRecord.class, AvroObjectPrepend.class)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }


    @Override
    public ProducerRecord<byte[], byte[]> serialize(WeatherData element, KafkaSinkContext kafkaSinkContext, Long aLong) {
        try {
            String json = mapper.writeValueAsString(element);
            byte[] jsonByteArray = json.getBytes();
            long payloadTS = (Long) element.get("timestamp");
            String key = element.getDeviceId().toString();
            return new ProducerRecord<>(topic, null, payloadTS, key == null ? null : key.getBytes(), jsonByteArray);
        } catch (JsonProcessingException e) {
            logger.error("Exception while serializing avro record {}", element, e);
        }
        return null;
    }
    private static class AvroObjectPrepend {

        @JsonIgnore
        public Schema getSchema() {
            return null;
        }

        @JsonIgnore
        public SpecificData getSpecificData() {
            return null;
        }
    }
}