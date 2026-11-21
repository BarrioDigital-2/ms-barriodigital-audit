package cl.duoc.ms_barriodigital_audit.consumer;

import cl.duoc.ms_barriodigital_audit.dto.SolicitudEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AuditoriaConsumer {

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

        // TODO: persistir en tabla auditoria_eventos en MySQL
    }
}