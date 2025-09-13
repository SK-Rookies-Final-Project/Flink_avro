package com.example.flink.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;


public class MdsAuthorizeDenyPipeline {

    private static final String SOURCE_TOPIC = "confluent-audit-log-events";
    private static final String SINK_TOPIC   = "system-level-false";
    private static final String BOOTSTRAP_SERVERS = "ec2-3-143-42-149.us-east-2.compute.amazonaws.com:29092,ec2-3-143-42-149.us-east-2.compute.amazonaws.com:39092,ec2-3-143-42-149.us-east-2.compute.amazonaws.com:49092"; // TODO
    private static final String GROUP_ID = "mds-authorize-deny-pipeline";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter ISO_KST = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // ---------- 공통 SCRAM 설정 ----------
        Properties scramProps = new Properties();
        scramProps.setProperty("security.protocol", "SASL_PLAINTEXT");
        scramProps.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        scramProps.setProperty(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                        + "username=\"admin\" password=\"admin-secret\";"
        );
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(scramProps) // <- SCRAM 적용
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(SINK_TOPIC)
                        .setValueSerializationSchema((SerializationSchema<String>) element ->
                                element.getBytes(StandardCharsets.UTF_8))
                        .build())
                .setKafkaProducerConfig(scramProps)
                .build();

        DataStream<String> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "audit-source");

        DataStream<String> output = input
                // JSON 파싱 실패 레코드는 드롭
                .map((MapFunction<String, JsonNode>) value -> {
                    try {
                        return MAPPER.readTree(value);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter((FilterFunction<JsonNode>) n -> n != null)
                // 필터: type == authorization, data.methodName == mds.Authorize, granted == false
                .filter((FilterFunction<JsonNode>) n -> {
                    try {
                        String type = getText(n, "type");
                        String method = getText(n.path("data"), "methodName");
                        boolean granted = n.path("data").path("authorizationInfo").path("granted").asBoolean(true);
                        return "io.confluent.kafka.server/authorization".equals(type)
                                && "mds.Authorize".equals(method)
                                && !granted;
                    } catch (Exception e) {
                        return false;
                    }
                })
                // 변환: 요구한 필드만 추출, KST 변환
                .map((MapFunction<JsonNode, String>) n -> {
                    ObjectNode out = MAPPER.createObjectNode();

                    // granted
                    boolean granted = n.path("data").path("authorizationInfo").path("granted").asBoolean(false);
                    out.put("granted", granted);

                    // eventTime (KST)
                    String eventTimeRaw = getText(n, "time"); // e.g. 2025-09-10T07:21:14.471467727Z
                    String eventTimeKst = toKstIso(eventTimeRaw);
                    out.put("eventTime", eventTimeKst);

                    // principal
                    String principal = getText(n.path("data").path("authenticationInfo"), "principal");
                    out.put("principal", principal);

                    // clientIp (첫번째)
                    String clientIp = null;
                    JsonNode ca = n.path("data").path("clientAddress");
                    if (ca.isArray() && ca.size() > 0) {
                        clientIp = getText(ca.get(0), "ip");
                    }
                    if (clientIp != null) out.put("clientIp", clientIp); else out.put("clientIp", "");

                    // methodName
                    String methodName = getText(n.path("data"), "methodName");
                    out.put("methodName", methodName);

                    // resourceType
                    String resourceType = getText(n.path("data").path("authorizationInfo"), "resourceType");
                    out.put("resourceType", resourceType);

                    // dataResourceName
                    String dataResourceName = getText(n.path("data"), "resourceName");
                    out.put("dataResourceName", dataResourceName);

                    // authResourceName
                    String authResourceName = getText(n.path("data").path("authorizationInfo"), "resourceName");
                    if (authResourceName == null || authResourceName.isEmpty()) {
                        authResourceName = deriveNameFromCrn(dataResourceName);
                    }
                    out.put("authResourceName", authResourceName == null ? "" : authResourceName);

                    // processingTime (KST, 현재 시각)
                    String processingKst = ZonedDateTime.now(KST).format(ISO_KST);
                    out.put("processingTime", processingKst);

                    return MAPPER.writeValueAsString(out);
                });

        output.sinkTo(sink);

        env.execute("MDS Authorize Deny -> system-level-false");
    }

    private static String getText(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    private static String toKstIso(String zInstant) {
        try {
            // CloudEvent "time" 은 RFC3339/ISO-8601(UTC Z)
            Instant ins = Instant.parse(zInstant);
            return ins.atZone(KST).format(ISO_KST);
        } catch (Exception e) {
            // 파싱 실패 시 원본 반환(혹은 빈값)
            return zInstant;
        }
    }

    // data.resourceName (CRN)에서 마지막 "=값" 뽑기
    private static String deriveNameFromCrn(String resourceName) {
        if (resourceName == null) return null;
        // 예: crn:///kafka=Axno.../topic=test-topic  -> test-topic
        //     crn:///kafka=.../security-metadata=security-metadata -> security-metadata
        int idx = resourceName.lastIndexOf('=');
        if (idx >= 0 && idx + 1 < resourceName.length()) {
            return resourceName.substring(idx + 1);
        }
        return resourceName;
    }
}