package cl.duoc.ms_barriodigital_audit.config;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;

//SErdes es una clase de kafka que convierte de java a bytes y de vuelta
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import cl.duoc.ms_barriodigital_audit.dto.SolicitudEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    public static final String TOPIC = "solicitudes-eventos";
    public static final String STORE_POR_COMUNA = "solicitudes-por-comuna-store";
    public static final String STORE_POR_TIPO = "solicitudes-por-tipo-store";
    public static final Duration VENTANA = Duration.ofMinutes(60);

    private static final String SIN_COMUNA = "SIN_COMUNA";
    private static final String SIN_TIPO = "SIN_TIPO";

    @Bean
    public KStream<String, SolicitudEvent> dashboardTopology(StreamsBuilder builder) {

        // Serde JSON para nuestro DTO, ignorando el header __TypeId__ del producer
        JacksonJsonSerde<SolicitudEvent> eventoSerde = new JacksonJsonSerde<>(SolicitudEvent.class);
        eventoSerde.ignoreTypeHeaders();

        KStream<String, SolicitudEvent> eventos = builder
                .stream(TOPIC, Consumed.with(Serdes.String(), eventoSerde))
                .filter((key, evento) -> evento != null)
                .peek((key, evento) -> log.info("📊 [Dashboard] Procesando: tipo={}, comuna={}",
                        evento.getTipo(), evento.getComunaId()));

        // Rama 1: solicitudes por comuna en ventanas de 60 minutos
        eventos
                .groupBy((key, evento) -> valorODefault(evento.getComunaId(), SIN_COMUNA),
                        Grouped.with("por-comuna", Serdes.String(), eventoSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(VENTANA))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(STORE_POR_COMUNA)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        // Rama 2: total acumulado por tipo de evento
        eventos
                .groupBy((key, evento) -> valorODefault(evento.getTipo(), SIN_TIPO),
                        Grouped.with("por-tipo", Serdes.String(), eventoSerde))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STORE_POR_TIPO)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        return eventos;
    }

    private static String valorODefault(String valor, String porDefecto) {
        return (valor == null || valor.isBlank()) ? porDefecto : valor;
    }
}
