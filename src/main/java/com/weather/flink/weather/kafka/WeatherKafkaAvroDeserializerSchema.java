package com.weather.flink.weather.kafka;

import com.weather.info.avro.SchemaRegistry;
import com.weather.info.avro.WeatherData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
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
        byte[] message = inRecord.value();
        byte magic = message[0];
        try {
            logger.trace("Message size: {}, magic: {}", message.length, magic);
            Schema writerSchema = registry.getSchema(magic);
            if (!writerSchema.getName().equals(readerSchema.getName())) {
                logger.error("Schema name mismatch: inst: {}, reader {} and writer {}, magic:{} - skipping:", this, readerSchema.getName(), writerSchema.getName(), magic);

                decoder = DecoderFactory.get().binaryDecoder(Arrays.copyOfRange(message, 1, message.length), decoder);
                GenericDatumReader<GenericData.Record> datumReader = new GenericDatumReader<>(writerSchema);
                GenericData.Record genericRecord = datumReader.read(null, decoder);
                logger.error("Schema name mismatch: reader {} and writer {},{} - skipping: {}.", readerSchema.getName(), writerSchema.getName(), magic, genericRecord);
                return;
            }
            logger.trace("Deserializing from {} ({})", writerSchema, magic);

            decoder = DecoderFactory.get().binaryDecoder(Arrays.copyOfRange(message, 1, message.length), decoder);
            SpecificDatumReader<WeatherData> reader = new SpecificDatumReader<WeatherData>(writerSchema, readerSchema) {
                @Override
                protected void readField(Object r, Schema.Field f, Object oldDatum, ResolvingDecoder in, Object state) throws IOException {
                    try {
                        super.readField(r, f, oldDatum, in, state);
                    } catch (Exception e) {
                        logger.error("*********Exception while reading field: {} and exception is {} ", f, e);
                        throw e;
                    }
                }
            };
            WeatherData output = reader.read(null, decoder);
            out.collect(output);
        } catch (Exception e) {
            logger.error("Ignoring weather paylod from registry index: {}, due to deserialization exception", magic, e);
        }
    }
}
