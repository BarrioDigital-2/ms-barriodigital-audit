package cl.duoc.ms_barriodigital_audit;

import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication

public class MsBarriodigitalAuditApplication {

	public static void main(String[] args) {
		SpringApplication.run(MsBarriodigitalAuditApplication.class, args);
	}

}
