package com.weather.flink.weather.kafka;

import com.weather.info.avro.SchemaRegistry;
import com.weather.info.avro.WeatherData;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class WeatherKafkaAvroDeserializerSchema implements KafkaRecordDeserializationSchema<WeatherData> {
    public static final Logger logger = LoggerFactory.getLogger(WeatherKafkaAvroDeserializerSchema.class);
    private static final long serialVersionUID = -8755372893283232018L;
    private transient SchemaRegistry registry;
    private transient BinaryDecoder decoder;
    private transient Schema readerSchema = WeatherData.SCHEMA$;
    private transient TypeInformation<WeatherData> typeInfo = TypeExtractor.getForClass(WeatherData.class);
    @Override
    public TypeInformation<WeatherData> getProducedType() {
        return typeInfo;
    }
    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        KafkaRecordDeserializationSchema.super.open(context);
        readerSchema = WeatherData.SCHEMA$;
        typeInfo = TypeExtractor.getForClass(WeatherData.class);
        registry = new SchemaRegistry();
    }
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> inRecord, Collector<WeatherData> out) throws IOException {
        try {
            byte[] message = inRecord.value();
            byte magic = message[0];
            logger.trace("");
            Schema writerSchema = registry.getSchema(magic);
            if (!writerSchema.getName().equals(readerSchema.getName())) {
                logger.error("Mismatch schema name, reader {} and writer {}", readerSchema.getName(), writerSchema.getName());
                return;
            }
            logger.trace("Message size: {}, magic: {}, Deserializing data {}", message.length, magic, writerSchema);
            decoder = DecoderFactory.get().binaryDecoder(Arrays.copyOfRange(message, 1, message.length), decoder);
            SpecificDatumReader<WeatherData> reader = new SpecificDatumReader<WeatherData>(writerSchema, readerSchema) {
                @Override
                protected void readField(Object r, Schema.Field f, Object oldDatum, ResolvingDecoder in, Object state) throws IOException {
                    try {
                        super.readField(r, f, oldDatum, in, state);
                    } catch (Exception e) {
                        logger.error("Exception while reading field: {} and exception is {} ", f, e);
                        throw e;
                    }
                }
            };
            out.collect(reader.read(null, decoder));
        } catch (Exception e) {
            logger.error("Ignoring weather paylod, exception while deserializing.", e);
        }
    }
}
