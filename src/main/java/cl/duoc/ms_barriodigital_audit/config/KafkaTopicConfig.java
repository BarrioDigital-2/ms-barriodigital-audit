package cl.duoc.ms_barriodigital_audit.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

// Topics donde publica audit. Spring los crea al arrancar si no existen
@Configuration
public class KafkaTopicConfig {

    public static final String TIMELINE_TOPIC = "audit.timeline";
    public static final String DLT_TOPIC = "audit.DLT";

    private static final String RETENCION_30_DIAS = "2592000000";
    private static final String RETENCION_14_DIAS = "1209600000";

    // En local 1 réplica (un solo broker); en AWS se define KAFKA_REPLICAS=3
    @Value("${KAFKA_REPLICAS:1}")
    private int replicas;

    // Historial de cada trámite. compact,delete: conserva el último evento de cada id y borra lo que pasa de 30 días
    @Bean
    public NewTopic auditTimelineTopic() {
        return TopicBuilder.name(TIMELINE_TOPIC)
                .partitions(3)
                .replicas(replicas)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG,
                        TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, RETENCION_30_DIAS)
                .build();
    }

    // Mensajes que fallaron tras los reintentos. Mismas 3 particiones que requests.events
    @Bean
    public NewTopic auditDltTopic() {
        return TopicBuilder.name(DLT_TOPIC)
                .partitions(3)
                .replicas(replicas)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, RETENCION_14_DIAS)
                .build();
    }
}
