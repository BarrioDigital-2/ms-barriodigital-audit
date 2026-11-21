package cl.duoc.ms_barriodigital_audit.consumer;

import cl.duoc.ms_barriodigital_audit.config.KafkaTopicConfig;
import cl.duoc.ms_barriodigital_audit.dto.AuditoriaRegistro;
import cl.duoc.ms_barriodigital_audit.dto.SolicitudEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
public class AuditoriaConsumer {

    private final KafkaTemplate<String, AuditoriaRegistro> kafkaTemplate;

    public AuditoriaConsumer(KafkaTemplate<String, AuditoriaRegistro> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
            topics = "${audit.kafka.topic}",
            groupId = "auditoria-processor"
    )
    public void consumir(SolicitudEvent evento) {
        // Un evento sin id no se puede auditar: la excepción activa los reintentos y luego el DLT
        if (evento.getId() == null) {
            throw new IllegalArgumentException("Evento sin id de solicitud");
        }

        log.info("📋 [Auditoría] Evento recibido: tipo={}, solicitudId={}, comuna={}",
                evento.getTipo(), evento.getId(), evento.getComunaId());

        AuditoriaRegistro registro = new AuditoriaRegistro(
                evento.getId(),
                evento.getTipo(),
                evento.getTitulo(),
                evento.getEstado(),
                evento.getComunaId(),
                evento.getTimestamp(),
                LocalDateTime.now().toString());

        // Key = id del trámite: la compactación deja el último registro de cada trámite
        kafkaTemplate.send(KafkaTopicConfig.TIMELINE_TOPIC, String.valueOf(evento.getId()), registro);
        log.info("🗂️ [Auditoría] Registrado en {}: solicitudId={}, estado={}",
                KafkaTopicConfig.TIMELINE_TOPIC, evento.getId(), evento.getEstado());
    }
}
