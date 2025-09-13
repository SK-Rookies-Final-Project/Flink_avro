package com.example.flink.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class AuditUpsertKafkaJob {

    // ===== 환경 설정 =====
    private static final String KAFKA_BOOTSTRAP =
            "ec2-3-143-42-149.us-east-2.compute.amazonaws.com:29092," +
                    "ec2-3-143-42-149.us-east-2.compute.amazonaws.com:39092," +
                    "ec2-3-143-42-149.us-east-2.compute.amazonaws.com:49092";

    private static final String SRC_TOPIC   = "confluent-audit-log-events";
    private static final String AVRO_TOPIC  = "resource-level-false-avro";   // upsert/compaction 권장
    private static final String JSON_TOPIC  = "resource-level-false-json";   // 사람 눈으로 확인용

    // Schema Registry (직렬화기는 보통 1개 URL 사용)
    private static final List<String> SR_URLS = Arrays.asList(
            "http://ec2-3-143-42-149.us-east-2.compute.amazonaws.com:18081",
            "http://ec2-3-143-42-149.us-east-2.compute.amazonaws.com:18082"
    );
    private static final String SR_URL = SR_URLS.get(0);

    // SASL 필요 시 사용
    private static final Properties KAFKA_SECURITY_PROPS = new Properties();
    static {
        KAFKA_SECURITY_PROPS.put("security.protocol", "SASL_PLAINTEXT"); // or SASL_SSL
        KAFKA_SECURITY_PROPS.put("sasl.mechanism", "SCRAM-SHA-512");
        KAFKA_SECURITY_PROPS.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                        "username=\"admin\" password=\"admin-secret\";");
    }

    // KST 포맷터
    private static final DateTimeFormatter KST_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("Asia/Seoul"));

    // ===== Avro 스키마 (key/value) =====
    private static final String KEY_SCHEMA_STR =
            "{ \"type\":\"record\", \"name\":\"EventKey\", \"fields\":[ {\"name\":\"id\",\"type\":\"string\"} ] }";

    private static final String VALUE_SCHEMA_STR =
            "{"
                    + "\"type\":\"record\","
                    + "\"name\":\"ResourceLevelFalse\","
                    + "\"namespace\":\"com.seoul.audit\","
                    + "\"fields\":["
                    + " {\"name\":\"id\",\"type\":\"string\"},"
                    + " {\"name\":\"event_time_utc\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
                    + " {\"name\":\"event_time_kst\",\"type\":\"string\"},"
                    + " {\"name\":\"process_time_utc\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
                    + " {\"name\":\"process_time_kst\",\"type\":\"string\"},"
                    + " {\"name\":\"principal\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + " {\"name\":\"role\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + " {\"name\":\"operation\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + " {\"name\":\"action\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + " {\"name\":\"method_name\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + " {\"name\":\"resource_name\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + " {\"name\":\"client_id\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + " {\"name\":\"client_ip\",\"type\":[\"null\",\"string\"],\"default\":null}"
                    + "]"
                    + "}";

    public static void main(String[] args) throws Exception {
        // === Flink 환경 ===
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
        env.getCheckpointConfig().enableUnalignedCheckpoints(true);
        env.getCheckpointConfig().setCheckpointTimeout(120_000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new HashMapStateBackend());

        // === Kafka Source(JSON) ===
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(SRC_TOPIC)
                .setGroupId("flink-audit-upsert")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(KAFKA_SECURITY_PROPS)
                .build();

        DataStream<String> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "audit-json");
        final ObjectMapper mapper = new ObjectMapper();

        DataStream<ReducedEvent> filtered = raw
                .map((MapFunction<String, ReducedEvent>) value -> {
                    try {
                        JsonNode root = mapper.readTree(value);
                        JsonNode data = root.path("data");
                        JsonNode authz = data.path("authorizationInfo");
                        boolean granted = authz.path("granted").asBoolean(false);
                        String resourceType = authz.path("resourceType").asText(null);
                        String operation = authz.path("operation").asText(null);

                        if (granted) return null;
                        if (!"Topic".equals(resourceType)) return null;
                        if (!Arrays.asList("Read","Write","Describe").contains(operation)) return null;

                        String id = root.path("id").asText(); // CloudEvents id
                        String methodName = data.path("methodName").asText(null);
                        String principal = data.path("authenticationInfo").path("principal").asText(null);
                        String role = authz.path("rbacAuthorization").path("role").asText(null);
                        String resourceName = authz.path("resourceName").asText(null);
                        String clientId = data.path("request").path("client_id").asText(null);

                        String clientIp = null;
                        if (data.path("clientAddress").isArray() && data.path("clientAddress").size() > 0) {
                            clientIp = data.path("clientAddress").get(0).path("ip").asText(null);
                        }

                        Instant eventTime = Instant.parse(root.path("time").asText());
                        String action = "other";
                        if ("Read".equals(operation)) action = "consume";
                        else if ("Write".equals(operation)) action = "produce";
                        else if ("Describe".equals(operation)) action = "describe";

                        Instant now = Instant.now();

                        return new ReducedEvent(
                                id,
                                eventTime.toEpochMilli(),
                                KST_FMT.format(eventTime),
                                now.toEpochMilli(),
                                KST_FMT.format(now),
                                principal, role, operation, action,
                                methodName, resourceName, clientId, clientIp
                        );
                    } catch (Exception e) {
                        return null; // 파싱 실패 시 드랍(필요시 DLQ)
                    }
                })
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ReducedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((e, ts) -> e.eventTimeUtcMillis)
                );

        // === Avro(+SR) Sink (upsert) ===
        final Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_STR);
        final Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_STR);

        // (선택) 사전 등록
        SchemaRegistryClient srClient = new CachedSchemaRegistryClient(SR_URLS, 100);
        final String subjectKey = AVRO_TOPIC + "-key";
        final String subjectValue = AVRO_TOPIC + "-value";
        try { srClient.register(subjectKey, new AvroSchema(keySchema)); } catch (Exception ignored) {}
        try { srClient.register(subjectValue, new AvroSchema(valueSchema)); } catch (Exception ignored) {}

        SerializationSchema<ReducedEvent> keySer =
                new ReducedToAvroSerializer(keySchema, subjectKey, SR_URL, true);
        SerializationSchema<ReducedEvent> valueSer =
                new ReducedToAvroSerializer(valueSchema, subjectValue, SR_URL, false);

        KafkaSink<ReducedEvent> avroSink = KafkaSink.<ReducedEvent>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<ReducedEvent>builder()
                                .setTopic(AVRO_TOPIC)
                                .setKeySerializationSchema(keySer)
                                .setValueSerializationSchema(valueSer)
                                .build()
                )
                .setKafkaProducerConfig(KAFKA_SECURITY_PROPS)
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        filtered.sinkTo(avroSink).name("upsert-kafka-avro-sr");

        // === JSON Sink (사람이 보기 쉬운 값) ===
        final ObjectMapper json = new ObjectMapper();

        SerializationSchema<ReducedEvent> jsonValueSer = new SerializationSchema<>() {
            @Override public byte[] serialize(ReducedEvent e) {
                try {
                    var node = json.createObjectNode();
                    node.put("id", e.id);
                    node.put("event_time_utc", e.eventTimeUtcMillis);
                    node.put("event_time_kst", e.eventTimeKst);
                    node.put("process_time_utc", e.processTimeUtcMillis);
                    node.put("process_time_kst", e.processTimeKst);
                    node.put("principal", e.principal);
                    node.put("role", e.role);
                    node.put("operation", e.operation);
                    node.put("action", e.action);
                    node.put("method_name", e.methodName);
                    node.put("resource_name", e.resourceName);
                    node.put("client_id", e.clientId);
                    node.put("client_ip", e.clientIp);
                    return json.writeValueAsBytes(node);
                } catch (Exception ex) {
                    return null;
                }
            }
        };

        SerializationSchema<ReducedEvent> jsonKeySer =
                e -> e.id == null ? null : e.id.getBytes(StandardCharsets.UTF_8);

        KafkaSink<ReducedEvent> jsonSink = KafkaSink.<ReducedEvent>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<ReducedEvent>builder()
                                .setTopic(JSON_TOPIC)
                                .setKeySerializationSchema(jsonKeySer)   // compaction 원치 않으면 제거 가능
                                .setValueSerializationSchema(jsonValueSer)
                                .build()
                )
                .setKafkaProducerConfig(KAFKA_SECURITY_PROPS)
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        filtered.sinkTo(jsonSink).name("human-readable-json-sink");

        env.execute("audit-upsert-kafka-dual-sink");
    }

    // ===== ReducedEvent → Avro(+SR) 직렬화기 =====
    public static class ReducedToAvroSerializer implements SerializationSchema<ReducedEvent> {
        private final Schema schema;
        private final String subject;
        private final String srUrl;
        private final boolean key;
        private transient SerializationSchema<GenericRecord> delegate;

        public ReducedToAvroSerializer(Schema schema, String subject, String srUrl, boolean key) {
            this.schema = schema;
            this.subject = subject;
            this.srUrl = srUrl;
            this.key = key;
        }

        @Override
        public void open(InitializationContext context) {
            // Flink 1.20: (subject, schema, url)
            this.delegate = ConfluentRegistryAvroSerializationSchema.forGeneric(subject, schema, srUrl);
        }

        @Override
        public byte[] serialize(ReducedEvent e) {
            GenericRecord gr = new GenericData.Record(schema);
            if (key) {
                gr.put("id", e.id);
            } else {
                gr.put("id", e.id);
                gr.put("event_time_utc", e.eventTimeUtcMillis);
                gr.put("event_time_kst", e.eventTimeKst);
                gr.put("process_time_utc", e.processTimeUtcMillis);
                gr.put("process_time_kst", e.processTimeKst);
                gr.put("principal", e.principal);
                gr.put("role", e.role);
                gr.put("operation", e.operation);
                gr.put("action", e.action);
                gr.put("method_name", e.methodName);
                gr.put("resource_name", e.resourceName);
                gr.put("client_id", e.clientId);
                gr.put("client_ip", e.clientIp);
            }
            return delegate.serialize(gr);
        }
    }

    // ===== POJO =====
    public static class ReducedEvent {
        public String id;
        public long eventTimeUtcMillis;
        public String eventTimeKst;
        public long processTimeUtcMillis;
        public String processTimeKst;
        public String principal;
        public String role;
        public String operation;
        public String action;
        public String methodName;
        public String resourceName;
        public String clientId;
        public String clientIp;

        public ReducedEvent() {}

        public ReducedEvent(String id, long eventTimeUtcMillis, String eventTimeKst,
                            long processTimeUtcMillis, String processTimeKst,
                            String principal, String role, String operation, String action,
                            String methodName, String resourceName, String clientId, String clientIp) {
            this.id = id;
            this.eventTimeUtcMillis = eventTimeUtcMillis;
            this.eventTimeKst = eventTimeKst;
            this.processTimeUtcMillis = processTimeUtcMillis;
            this.processTimeKst = processTimeKst;
            this.principal = principal;
            this.role = role;
            this.operation = operation;
            this.action = action;
            this.methodName = methodName;
            this.resourceName = resourceName;
            this.clientId = clientId;
            this.clientIp = clientIp;
        }
    }
}