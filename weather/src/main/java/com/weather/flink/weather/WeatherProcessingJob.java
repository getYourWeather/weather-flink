package com.weather.flink.weather;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.*;
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
    private final String tenantId;
    protected StreamExecutionEnvironment environment;
    public static final String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";

    public WeatherProcessingJob() {
        String serverConfig = jobParameters.getConfiguration().get(ConfigOptions.key("kafka.servers").stringType().defaultValue("kafka:9092"));
        tenantId = jobParameters.getConfiguration().get(ConfigOptions.key("tenantId").stringType().defaultValue(""));
        consumerProps = new Properties();
        consumerProps.setProperty(KAFKA_BOOTSTRAP_SERVERS, serverConfig);
        consumerProps.put("enable.auto.commit", true);
        consumerProps.put("client.dns.lookup", "use_all_dns_ips");
        consumerProps.put("reconnect.backoff.ms", 1000);
        consumerProps.put("reconnect.backoff.max.ms", 5000);
        producerProps = new Properties();
        producerProps.setProperty(KAFKA_BOOTSTRAP_SERVERS, serverConfig);
    }

    public static void main(String[] args) throws Exception {
        jobParameters = ParameterTool.fromArgs(args);
        jobParameters = jobParameters.mergeWith(ParameterTool.fromSystemProperties());

        if (logger.isInfoEnabled()) {
            logger.info("Weather Job Settings:\n{}", jobParameters.getConfiguration());
        }

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
        environment.setParallelism(2);
        environment.getConfig().setAutoWatermarkInterval(200L);
    }

    private DataStream<WeatherData> getDataStream() {
        String weatherTopic = getTopic();
        final Float outOfOrderInMillisFloat = 3.5f * 1000;
        final long outOfOrderInMillis = outOfOrderInMillisFloat.longValue();
        KafkaSource<WeatherData> kafkaDataSource = KafkaSource
                .<WeatherData>builder()
                .setBootstrapServers(consumerProps.getProperty(KAFKA_BOOTSTRAP_SERVERS))
                .setProperties(consumerProps)
                .setTopics(weatherTopic)
                .setDeserializer(null).setGroupId(GROUP_ID)
                .build();
        return environment.fromSource(kafkaDataSource,
                WatermarkStrategy.<WeatherData>forBoundedOutOfOrderness(Duration.ofMillis(outOfOrderInMillis)).withIdleness(Duration.ofMillis(500)), "weather Event data source").uid("weather Event data source");
    }

    public String getTopic() {
        StringBuilder topic = new StringBuilder("weatherData");
        if (StringUtils.isNotBlank(tenantId)) {
            topic.append("-").append(tenantId);
        }
        return topic.toString();
    }

    private void setupExecutionPlan() {
        DataStream<WeatherData> inStream = getDataStream();
        // create the job plan
    }

    private void execute() throws Exception {
        logger.info("Execution plan:\n {} \n", environment.getExecutionPlan());
        environment.execute("weatherProcessingJob");
    }
}
