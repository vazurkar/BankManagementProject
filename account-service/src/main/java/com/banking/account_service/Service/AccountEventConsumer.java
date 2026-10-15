package com.banking.account_service.Service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
public class AccountEventConsumer {
    private final AccountService accountService;
    public AccountEventConsumer(AccountService accountService) {
        this.accountService = accountService;
    }


        //consume transaction.completed event from kafka credits receiver amount param payload
   @KafkaListener(topics = "transaction.completed" , groupId = "account-service-group")
    public void consumeTransactionCompleted(
            @Payload Map<String,Object> payload
    ){
        try{
            String receiverAccountNumber = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = (BigDecimal) payload.get("amount");
            log.info("Crediting account : {} amount : {}", receiverAccountNumber, amount);
            accountService.creditBalance(receiverAccountNumber, amount);
        }catch (Exception e){
            log.error("Error credting account : {}", e.getMessage());
        }
    }

    //consume fraud.detected events from kafka and it blocks the flagged account

    @KafkaListener(topics = "fraud.detected"  , groupId = "account-service-group")
    public void consumeFraudDetected(
            @Payload Map<String,Object> payload
    ){
        try{
            String receiverAccountNumber = (String) payload.get("receiverAccountNumber");
            log.info("Fraud detected blocking account: {}",receiverAccountNumber);
            accountService.blockAccount(receiverAccountNumber);

        }catch (Exception e){
            log.error("Error blocking account : {}", e.getMessage());
        }
    }

}
