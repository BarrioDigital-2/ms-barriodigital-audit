package cl.duoc.ms_barriodigital_audit.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SolicitudEvent {

    private Long id;
    private String tipo;
    private String comunaId;
    private String titulo;
    private String descripcion;
    private String estado;
    private String timestamp;
    private String tipo_tramite;
}