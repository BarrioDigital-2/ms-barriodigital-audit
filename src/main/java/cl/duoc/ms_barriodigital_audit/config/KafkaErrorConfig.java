package cl.duoc.ms_barriodigital_audit.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
public class KafkaErrorConfig {

    // Si un evento falla: 3 reintentos con 1 segundo entre cada uno; si sigue fallando, va a audit.DLT
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> jsonTemplate,
                                            ProducerFactory<Object, Object> producerFactory) {
        // Un JSON inválido no se pudo convertir a objeto: viaja al DLT como bytes, tal como llegó
        KafkaTemplate<Object, Object> bytesTemplate = new KafkaTemplate<>(producerFactory,
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));

        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);
        templates.put(Object.class, jsonTemplate);

        // Va a la misma partición de origen (por eso audit.DLT tiene 3 particiones)
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(templates,
                (registro, error) -> new TopicPartition(KafkaTopicConfig.DLT_TOPIC, registro.partition()));

        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
    }
}
