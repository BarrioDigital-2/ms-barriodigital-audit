package cl.duoc.ms_barriodigital_audit.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// Lo que audit publica en audit.timeline por cada evento procesado
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AuditoriaRegistro {

    private Long solicitudId;
    private String evento;          // SOLICITUD_CREADA o SOLICITUD_ACTUALIZADA
    private String tipoTramite;     // ej. PODA_ARBOLES
    private String estado;          // estado del trámite después del evento
    private String direccion;
    private String fechaEvento;     // cuándo ocurrió (según requests)
    private String fechaAuditoria;  // cuándo lo registró audit
}
