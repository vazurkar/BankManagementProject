package com.banking.fraud_detectionservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

@SpringBootApplication
@EnableFeignClients
public class FraudDetectionserviceApplication {

	public static void main(String[] args) {
		SpringApplication.run(FraudDetectionserviceApplication.class, args);
	}

	@Bean
	public RedisTemplate<String, Object> redisTemplate(
			RedisConnectionFactory connectionFactory) {

		RedisTemplate<String, Object> template =
				new RedisTemplate<>();

		template.setConnectionFactory(connectionFactory);

		return template;
	}

}
