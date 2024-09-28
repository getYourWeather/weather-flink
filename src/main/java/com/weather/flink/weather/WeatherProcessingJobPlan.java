package com.weather.flink.weather;

import com.weather.info.avro.WeatherData;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;

public class WeatherProcessingJobPlan {

    public void executionPlan(DataStream<WeatherData> inStream) {
        // process
    }

    public void setKafkaSink(KafkaSink<WeatherData> KafkaSink) {
        // presence event sink
    }
}
