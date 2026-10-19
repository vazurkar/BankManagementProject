package com.banking.fraud_detectionservice.Service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.extern.slf4j.XSlf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionEventConsumer {
    private final FraudDetectionService fraudDetectionService;

    /**
     * listen to transaction.initiated topic
     * every transaction goes through fraud check before complete
     * @param payload
     */

    @KafkaListener(topics = "transaction.initiated" , groupId = "fraud-detection-group")
    public void consumeTransactionInitiated(
            @Payload Map<String , Object> payload

            ){
        log.info("Transaction received for fraud check : {}", payload.get("transactionId"));
        try{
        fraudDetectionService.checkTransaction(payload);

        }catch (Exception e) {
            log.error("Error while processing transaction {}", payload.get("transactionId"), e);
        }
    }
}
