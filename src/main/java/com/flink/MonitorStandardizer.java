package com.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

public class MonitorStandardizer {

    private static final String KAFKA_BOOTSTRAP_SERVERS =
            "172.16.10.27:9092,172.16.10.28:9092,172.16.10.29:9092";
    private static final String KAFKA_TOPIC = "prometheus-metrics";
    private static final String KAFKA_GROUP_ID = "monitor-standardizer-v3";

    private static final String DWS_URL =
            "jdbc:postgresql://172.31.251.211:8000/dws";
    private static final String DWS_USERNAME = "dbadmin";
    private static final String DWS_PASSWORD = "Longcheer@2025!!";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Set<String> MANDATORY_LABELS = new HashSet<>(Arrays.asList(
            "instance", "department", "project", "env",
            "resource_category", "resource_type", "service",
            "region", "criticality", "owner"
    ));

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(60000);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        kafkaProps.setProperty("group.id", KAFKA_GROUP_ID);
        kafkaProps.setProperty("auto.offset.reset", "latest");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                KAFKA_TOPIC,
                new SimpleStringSchema(),
                kafkaProps
        );

        DataStream<String> rawStream = env.addSource(kafkaSource);

        rawStream.addSink(JdbcSink.sink(
                "INSERT INTO test.prometheus_monitor_fact "
                + "(ts, metric_name, metric_value, "
                + "instance, department, project, env, "
                + "resource_category, resource_type, service, "
                + "region, criticality, owner, "
                + "ext_tags, kafka_topic, kafka_partition, kafka_offset) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)",
                (PreparedStatement ps, String rawJson) -> {
                    JsonNode root = null;
                    try {
                        root = MAPPER.readTree(rawJson);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }

                    long ts = 0L;
                    if (root.has("timestamp")) {
                        ts = Instant.parse(root.get("timestamp").asText()).toEpochMilli();
                    }
                    String metricName = root.has("name") ? root.get("name").asText() : "";
                    double metricValue = root.has("value")
                            ? Double.parseDouble(root.get("value").asText()) : 0.0;

                    Map<String, String> labels = new HashMap<>();
                    Map<String, String> extLabels = new HashMap<>();
                    if (root.has("labels")) {
                        JsonNode labelsNode = root.get("labels");
                        Iterator<Map.Entry<String, JsonNode>> fields = labelsNode.fields();
                        while (fields.hasNext()) {
                            Map.Entry<String, JsonNode> entry = fields.next();
                            String key = entry.getKey();
                            String val = entry.getValue().asText();
                            if (MANDATORY_LABELS.contains(key)) {
                                labels.put(key, val);
                            } else if (!"__name__".equals(key)) {
                                extLabels.put(key, val);
                            }
                        }
                    }

                    ps.setTimestamp(1, new Timestamp(ts));
                    ps.setString(2, metricName);
                    ps.setDouble(3, metricValue);

                    ps.setString(4, labels.getOrDefault("instance", "unknown"));
                    ps.setString(5, labels.getOrDefault("department", "unknown"));
                    ps.setString(6, labels.getOrDefault("project", "unknown"));
                    ps.setString(7, labels.getOrDefault("env", "unknown"));
                    ps.setString(8, labels.getOrDefault("resource_category", "unknown"));
                    ps.setString(9, labels.getOrDefault("resource_type", "unknown"));
                    ps.setString(10, labels.getOrDefault("service", "unknown"));
                    ps.setString(11, labels.getOrDefault("region", "unknown"));
                    ps.setString(12, labels.getOrDefault("criticality", "unknown"));
                    ps.setString(13, labels.getOrDefault("owner", "unknown"));

                    try {
                        ps.setString(14, MAPPER.writeValueAsString(extLabels));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    ps.setString(15, KAFKA_TOPIC);
                    ps.setInt(16, 0);
                    ps.setLong(17, 0);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(DWS_URL)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(DWS_USERNAME)
                        .withPassword(DWS_PASSWORD)
                        .build()
        ));

        env.execute("MonitorStandardizer");
    }
}