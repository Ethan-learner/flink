package com.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

public class PrometheusMetricsToDoris {

    // Kafka 配置
    private static final String KAFKA_BOOTSTRAP_SERVERS = "172.16.10.27:9092,172.16.10.28:9092,172.16.10.29:9092";
    private static final String KAFKA_TOPIC = "prometheus-metrics";
    private static final String KAFKA_GROUP_ID = "prometheus-metrics-to-doris-group";

    // Doris 配置
    private static final String DORIS_URL = "jdbc:mysql://10.206.4.154:9030/test?rewriteBatchedStatements=true&useServerPrepStmts=false";
    private static final String DORIS_USERNAME = "root";
    private static final String DORIS_PASSWORD = "YB!R,!(aZSH)GOW";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Set<String> MANDATORY_LABELS = new HashSet<>(Arrays.asList(
            "instance", "department", "project", "env",
            "resource_category", "resource_type", "service",
            "region", "criticality", "owner"
    ));

    // 定义一个内部类来承载解析后的数据和元数据
    public static class MetricData {
        long tsMillis;
        String metricName;
        double metricValue;
        Map<String, String> labels;
        int partition;
        long offset;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(9);
        env.enableCheckpointing(60000);

        // 使用新的 KafkaSource 配合 KafkaRecordDeserializationSchema 获取元数据
        KafkaSource<MetricData> kafkaSource = KafkaSource.<MetricData>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(KAFKA_TOPIC)
                .setGroupId(KAFKA_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new KafkaRecordDeserializationSchema<MetricData>() {
                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<MetricData> out) {
                        try {
                            String rawJson = new String(record.value());
                            MetricData data = new MetricData();

                            // 提取 Kafka 真实的分区号和偏移量
                            data.partition = record.partition();
                            data.offset = record.offset();

                            JsonNode root = MAPPER.readTree(rawJson);

                            if (root.has("timestamp")) {
                                data.tsMillis = Instant.parse(root.get("timestamp").asText()).toEpochMilli();
                            }

                            data.metricName = root.has("name") ? root.get("name").asText() : "";

                            if (root.has("value") && !root.get("value").isNull()) {
                                try {
                                    String valStr = root.get("value").asText().trim();
                                    if (!valStr.isEmpty() && !valStr.equalsIgnoreCase("NaN") && !valStr.equals("-")) {
                                        data.metricValue = Double.parseDouble(valStr);
                                    }
                                } catch (NumberFormatException e) {
                                    System.err.println("警告：发现非法的 metric_value 数据 -> " + root.get("value").asText());
                                }
                            }

                            data.labels = new HashMap<>();
                            if (root.has("labels")) {
                                JsonNode labelsNode = root.get("labels");
                                Iterator<Map.Entry<String, JsonNode>> fields = labelsNode.fields();
                                while (fields.hasNext()) {
                                    Map.Entry<String, JsonNode> entry = fields.next();
                                    String key = entry.getKey();
                                    String val = entry.getValue().asText();
                                    if (MANDATORY_LABELS.contains(key)) {
                                        data.labels.put(key, val);
                                    }
                                }
                            }
                            out.collect(data);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public TypeInformation<MetricData> getProducedType() {
                        return TypeInformation.of(MetricData.class);
                    }
                })
                .build();

        DataStream<MetricData> processedStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        processedStream.addSink(JdbcSink.sink(
                "INSERT INTO test.prometheus_monitor_fact "
                + "(ts, metric_name, metric_value, "
                + "instance, department, project, env, "
                + "resource_category, resource_type, service, "
                + "region, criticality, owner, "
                + "kafka_topic, kafka_partition, kafka_offset) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (PreparedStatement ps, MetricData data) -> {
                    ps.setTimestamp(1, new Timestamp(data.tsMillis));
                    ps.setString(2, data.metricName);
                    ps.setDouble(3, data.metricValue);

                    ps.setString(4, data.labels.getOrDefault("instance", "unknown"));
                    ps.setString(5, data.labels.getOrDefault("department", "unknown"));
                    ps.setString(6, data.labels.getOrDefault("project", "unknown"));
                    ps.setString(7, data.labels.getOrDefault("env", "unknown"));
                    ps.setString(8, data.labels.getOrDefault("resource_category", "unknown"));
                    ps.setString(9, data.labels.getOrDefault("resource_type", "unknown"));
                    ps.setString(10, data.labels.getOrDefault("service", "unknown"));
                    ps.setString(11, data.labels.getOrDefault("region", "unknown"));
                    ps.setString(12, data.labels.getOrDefault("criticality", "unknown"));
                    ps.setString(13, data.labels.getOrDefault("owner", "unknown"));

                    ps.setString(14, KAFKA_TOPIC);
                    ps.setInt(15, data.partition);  // 写入真实的分区号
                    ps.setLong(16, data.offset);    // 写入真实的偏移量
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(5000)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(DORIS_URL)
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername(DORIS_USERNAME)
                        .withPassword(DORIS_PASSWORD)
                        .build()
        ));

        env.execute("PrometheusMetricsToDoris");
    }
}