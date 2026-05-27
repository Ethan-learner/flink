package com.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;

public class AlertRecordToDoris {

    // Kafka 配置
    private static final String KAFKA_BOOTSTRAP_SERVERS = "172.16.10.27:9092,172.16.10.28:9092,172.16.10.29:9092";
    private static final String KAFKA_TOPIC = "alert-records";
    private static final String KAFKA_GROUP_ID = "alert-to-doris-group";

    // Doris 配置
    private static final String DORIS_URL = "jdbc:mysql://10.206.4.154:9030/test";
    private static final String DORIS_USERNAME = "root";
    private static final String DORIS_PASSWORD = "YB!R,!(aZSH)GOW";

    // Doris 目标表名
    private static final String DORIS_TABLE = "alert_fact_table";

    // 去重时间窗口（毫秒）
    private static final long DEDUP_WINDOW_MS = 30000; // 30秒

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // 告警核心维度标签
    private static final Set<String> MANDATORY_LABELS = new HashSet<>(Arrays.asList(
            "instance", "department", "project", "env",
            "resource_category", "resource_type", "service",
            "region", "criticality", "owner"
    ));

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(60000);

        // 1. 配置 Kafka 数据源
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

        // 2. 解析 JSON 并转换为 JsonNode
        DataStream<JsonNode> jsonStream = rawStream.map(json -> {
            try {
                return MAPPER.readTree(json);
            } catch (Exception e) {
                System.err.println("解析 JSON 失败: " + json);
                return null;
            }
        }).filter(Objects::nonNull);

        // 3. 去重：按 instance 分组，30秒内相同状态只处理一次（不区分副本）
        DataStream<JsonNode> dedupStream = jsonStream
                .keyBy(node -> node.path("instance").asText())
                .process(new DedupProcessFunction());

        // 4. 写入 Doris
        dedupStream.addSink(JdbcSink.sink(
            "INSERT INTO " + DORIS_TABLE + " " +
            "(alert_time, alert_name, status, severity, summary, description, " +
            "instance, department, project, env, " +
            "resource_category, resource_type, service, " +
            "region, criticality, owner, " +
            "ext_labels, source_system) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",

            (PreparedStatement ps, JsonNode root) -> {
                String alertName = root.path("alertname").asText("");
                if (alertName.isEmpty()) {
                    System.err.println("Invalid message format, missing alertname: " + root);
                    return;
                }

                String status = root.path("status").asText("");

                // resolved 时用 ends_at，firing 时用 starts_at
                Timestamp alertTime = null;
                if ("resolved".equals(status) && root.has("ends_at")) {
                    String alertTimeStr = root.get("ends_at").asText();
                    try {
                        alertTime = Timestamp.from(java.time.Instant.parse(alertTimeStr));
                    } catch (Exception e) {
                        alertTime = new Timestamp(System.currentTimeMillis());
                    }
                } else if (root.has("starts_at")) {
                    String alertTimeStr = root.get("starts_at").asText();
                    try {
                        alertTime = Timestamp.from(java.time.Instant.parse(alertTimeStr));
                    } catch (Exception e) {
                        alertTime = new Timestamp(System.currentTimeMillis());
                    }
                } else {
                    alertTime = new Timestamp(System.currentTimeMillis());
                }

                String severity = "";
                if (root.has("labels") && root.get("labels").has("severity")) {
                    severity = root.get("labels").get("severity").asText();
                }

                String summary = "";
                String description = "";
                if (root.has("annotations")) {
                    JsonNode annotations = root.get("annotations");
                    if (annotations.has("summary")) {
                        summary = annotations.get("summary").asText();
                    }
                    if (annotations.has("description")) {
                        description = annotations.get("description").asText();
                    }
                }

                String sourceSystem = "prometheus";
                if (root.has("labels") && root.get("labels").has("source_system")) {
                    sourceSystem = root.get("labels").get("source_system").asText();
                }

                Map<String, String> labels = new HashMap<>();
                Map<String, String> extLabels = new HashMap<>();

                JsonNode labelsNode = root.get("labels");
                if (labelsNode != null) {
                    Iterator<Map.Entry<String, JsonNode>> fields = labelsNode.fields();
                    while (fields.hasNext()) {
                        Map.Entry<String, JsonNode> entry = fields.next();
                        String key = entry.getKey();
                        String val = entry.getValue().asText();
                        if (MANDATORY_LABELS.contains(key)) {
                            labels.put(key, val);
                        } else {
                            extLabels.put(key, val);
                        }
                    }
                }

                ps.setTimestamp(1, alertTime);
                ps.setString(2, alertName);
                ps.setString(3, status);
                ps.setString(4, severity);
                ps.setString(5, summary);
                ps.setString(6, description);
                ps.setString(7, labels.get("instance"));
                ps.setString(8, labels.get("department"));
                ps.setString(9, labels.get("project"));
                ps.setString(10, labels.get("env"));
                ps.setString(11, labels.get("resource_category"));
                ps.setString(12, labels.get("resource_type"));
                ps.setString(13, labels.get("service"));
                ps.setString(14, labels.get("region"));
                ps.setString(15, labels.get("criticality"));
                ps.setString(16, labels.get("owner"));

                try {
                    ps.setString(17, MAPPER.writeValueAsString(extLabels));
                } catch (JsonProcessingException e) {
                    ps.setString(17, "{}");
                }
                ps.setString(18, sourceSystem);

                ps.executeUpdate();
            },
            JdbcExecutionOptions.builder()
                     .withBatchSize(1)
                     .withBatchIntervalMs(0)
                     .withMaxRetries(3)
                     .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(DORIS_URL)
                    .withDriverName("com.mysql.cj.jdbc.Driver")
                    .withUsername(DORIS_USERNAME)
                    .withPassword(DORIS_PASSWORD)
                    .build()
        ));
        env.execute("AlertRecordToDoris");
    }

    /**
     * 去重处理函数
     * 按 instance 分组，30秒内相同状态只处理一次（不区分 replica）
     */
    public static class DedupProcessFunction extends KeyedProcessFunction<String, JsonNode, JsonNode> {

        private transient ValueState<Long> firingLastTime;
        private transient ValueState<Long> resolvedLastTime;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Long> firingDesc = new ValueStateDescriptor<>("firing-last-time", Long.class);
            ValueStateDescriptor<Long> resolvedDesc = new ValueStateDescriptor<>("resolved-last-time", Long.class);
            firingLastTime = getRuntimeContext().getState(firingDesc);
            resolvedLastTime = getRuntimeContext().getState(resolvedDesc);
        }

        @Override
        public void processElement(JsonNode value, Context ctx, Collector<JsonNode> out) throws Exception {
            String status = value.path("status").asText("");
            String instance = value.path("instance").asText();
            long currentTime = System.currentTimeMillis();

            if ("firing".equals(status)) {
                Long lastTime = firingLastTime.value();
                if (lastTime == null || (currentTime - lastTime) > DEDUP_WINDOW_MS) {
                    firingLastTime.update(currentTime);
                    System.out.println("处理 firing: " + instance);
                    out.collect(value);
                } else {
                    System.out.println("跳过重复 firing: " + instance);
                }
            } else if ("resolved".equals(status)) {
                Long lastTime = resolvedLastTime.value();
                if (lastTime == null || (currentTime - lastTime) > DEDUP_WINDOW_MS) {
                    resolvedLastTime.update(currentTime);
                    System.out.println("处理 resolved: " + instance);
                    out.collect(value);
                } else {
                    System.out.println("跳过重复 resolved: " + instance);
                }
            }
        }
    }
}