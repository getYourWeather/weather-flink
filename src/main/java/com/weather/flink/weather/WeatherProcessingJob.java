package com.weather.flink.weather;

import com.weather.flink.weather.kafka.WeatherKafkaAvroDeserializerSchema;
import com.weather.flink.weather.kafka.WeatherPresenceSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.*;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.connector.kafka.source.KafkaSource;
import com.weather.info.avro.WeatherData;


import java.time.Duration;
import java.util.Properties;

public class WeatherProcessingJob
{
    private static final Logger logger = LoggerFactory.getLogger(WeatherProcessingJob.class);
    protected static ParameterTool jobParameters;
    private static final String GROUP_ID = "weather-processing-job";
    protected final Properties consumerProps;
    protected final Properties producerProps;
    protected StreamExecutionEnvironment environment;
    public static final String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";

    public WeatherProcessingJob() {
        String serverConfig = jobParameters.getConfiguration().get(ConfigOptions.key("kafka.servers").stringType().defaultValue("localhost:9092"));
        consumerProps = new Properties();
        consumerProps.setProperty(KAFKA_BOOTSTRAP_SERVERS, serverConfig);
        consumerProps.put("enable.auto.commit", false);
        consumerProps.put("client.dns.lookup", "use_all_dns_ips");
        consumerProps.put("reconnect.backoff.ms", 1000);
        consumerProps.put("reconnect.backoff.max.ms", 5000);
        producerProps = new Properties();
        producerProps.setProperty(KAFKA_BOOTSTRAP_SERVERS, serverConfig);
    }

    public static void main(String[] args) throws Exception {
        jobParameters = ParameterTool.fromArgs(args);
        jobParameters = jobParameters.mergeWith(ParameterTool.fromSystemProperties());
        logger.info("Weather Job Settings:\n{}", jobParameters.getConfiguration());
        WeatherProcessingJob job = new WeatherProcessingJob();
        job.setupExecutionEnvironment();
        job.setupExecutionPlan();
        job.execute();
    }

    private void setupExecutionEnvironment() {
        Configuration config = new Configuration(jobParameters.getConfiguration());
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "exponential-delay");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF, Duration.ofSeconds(15));
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF, Duration.ofSeconds(360));
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER, 1.5);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR, 0.5);
        environment = StreamExecutionEnvironment.getExecutionEnvironment(config);
        environment.getConfig().setGlobalJobParameters(jobParameters);
        environment.setParallelism(1);
        environment.getConfig().setAutoWatermarkInterval(200L);
    }

    private DataStream<WeatherData> getDataStream() {
        final float outOfOrderInMillisFloat = 3.5f * 1000;
        KafkaSource<WeatherData> kafkaDataSource = KafkaSource
                .<WeatherData>builder()
                .setBootstrapServers(consumerProps.getProperty(KAFKA_BOOTSTRAP_SERVERS))
                .setProperties(consumerProps)
                .setTopics("weatherData")
                .setDeserializer(new WeatherKafkaAvroDeserializerSchema())
                .setGroupId(GROUP_ID)
                .build();
        return environment.fromSource(kafkaDataSource,
                WatermarkStrategy.noWatermarks(), "weather Event data source").uid("weather Event data source");
    }

    private void setupExecutionPlan() {
        DataStream<WeatherData> inStream = getDataStream();
        inStream.map(e -> {
            logger.error("DATA IS : {} ", e.getDeviceId());
            return e;
        });
        WeatherProcessingJobPlan weatherProcessingJobPlan = new WeatherProcessingJobPlan();
        weatherProcessingJobPlan.executionPlan(inStream);
        weatherProcessingJobPlan.setKafkaSink(getDronePresenceProducer("WeatherPresenceEvent"));
    }
    private void execute() throws Exception {
        logger.info("Job execution plan: {}", environment.getExecutionPlan());
        environment.execute("weatherProcessingJob");
    }
    protected KafkaSink<WeatherData> getDronePresenceProducer(String topic) {
        return KafkaSink.<WeatherData>builder()
                .setBootstrapServers(producerProps.getProperty(KAFKA_BOOTSTRAP_SERVERS))
                .setKafkaProducerConfig(producerProps)
                .setTransactionalIdPrefix(topic)
                .setRecordSerializer(new WeatherPresenceSerializationSchema(topic))
                .build();
    }
}
