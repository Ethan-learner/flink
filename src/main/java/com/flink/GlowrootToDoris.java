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
import java.util.Properties;

public class GlowrootToDoris {

    // 配置保持不变
    private static final String KAFKA_BOOTSTRAP_SERVERS = "172.16.10.27:9092,172.16.10.28:9092";
    private static final String KAFKA_TOPIC = "cassandra.test_cdc.test_table";
    private static final String KAFKA_GROUP_ID = "flink_doris_cdc_group";

    private static final String DORIS_URL = "jdbc:mysql://10.206.4.154:9030/test";
    private static final String DORIS_USERNAME = "root";
    private static final String DORIS_PASSWORD = "YB!R,!(aZSH)GOW";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(60000);

        // 1. Kafka Source 配置
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        kafkaProps.setProperty("group.id", KAFKA_GROUP_ID);
        kafkaProps.setProperty("auto.offset.reset", "latest"); // 注意：生产环境建议改为 earliest 测试历史数据

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                KAFKA_TOPIC,
                new SimpleStringSchema(),
                kafkaProps
        );

        DataStream<String> rawStream = env.addSource(kafkaSource);

        // 2. 处理逻辑与 Doris Sink
        rawStream.addSink(JdbcSink.sink(
                // 修复点 1: 移除了 Doris 不支持的 ON DUPLICATE KEY UPDATE
                // 请确保 Doris 表是 UNIQUE 模型，或者你接受可能存在的重复数据（后续通过聚合去重）
                "INSERT INTO test_table (id, name, updated_at) VALUES (?, ?, ?)",

                (PreparedStatement ps, String rawJson) -> {
                    try {
                        // 修复点 2: 增加了对控制字符的清洗，防止 JSON 解析崩溃
                        // ASCII 0-31 是控制字符，除了 \r\n\t 都干掉
                        String cleanJson = rawJson.replaceAll("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]", "");

                        JsonNode root = MAPPER.readTree(cleanJson);
                        String op = root.has("op") ? root.get("op").asText() : "";

                        // 跳过删除操作
                        if ("d".equals(op)) {
                            System.out.println("跳过删除操作: " + rawJson.substring(0, Math.min(50, rawJson.length())));
                            return; // 注意：这里 return 可能导致 Batch 空提交，建议监控 Doris 日志
                        }

                        JsonNode after = root.get("after");
                        if (after == null) return;

                        String id = after.has("id") && after.get("id").has("value")
                                ? after.get("id").get("value").asText() : null;
                        String name = after.has("name") && after.get("name").has("value")
                                ? after.get("name").get("value").asText() : null;
                        long updatedAt = after.has("updated_at") && after.get("updated_at").has("value")
                                ? after.get("updated_at").get("value").asLong() : System.currentTimeMillis();

                        // Doris 中如果字段为 NULL 可能会报错，这里做非空判断或设置默认值
                        ps.setString(1, id);
                        ps.setString(2, name != null ? name : "N/A");
                        ps.setLong(3, updatedAt);

                        ps.addBatch(); // 确保添加到批次
                    } catch (Exception e) {
                        // 修复点 3: 捕获异常并打印，防止任务因为单条脏数据挂掉
                        System.err.println("处理数据失败，已跳过: " + rawJson + " | 错误: " + e.getMessage());
                        e.printStackTrace();
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
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

        env.execute("Kafka Doris CDC Job");
    }
}