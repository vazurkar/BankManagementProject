package com.banking.transaction_service.Service;


import com.banking.transaction_service.Entity.Transaction;
import com.banking.transaction_service.Entity.TransactionStatuses;
import com.banking.transaction_service.Repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
@Service
public class TransactionEventConsumer {

    private static final String TRANSACTIONAL_OTP_GENERATED_TOPIC = "transactional.otp.generated";

    private final TransactionRepository transactionRepository;

    private final RedisTemplate<String, String> redisTemplate;

    private static final long OTP_EXPIRAY_MINUTES =5;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final TransactionService transactionService;

    /**
     * consume verifiaction required
     * generate otp and ask user to verify
     * @param payload
     */

    @KafkaListener(topics = "verification.required")
    public void consumeVerificationRequired(
            @Payload Map<String, Object> payload
            ){
        try{
                String transactionId = (String) payload.get("transactionId");
                String accountNumber = (String) payload.get("accountNumber");
                String reason = payload.get("reason").toString();
                log.info("Transaction received for verification required : {} reason {} ", transactionId, reason
                );

            Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(() ->
                    new RuntimeException("Transaction not found "+ transactionId));
            if(transaction.getStatus() != TransactionStatuses.PROCESSING){
                log.warn("Transaction {} not processing - skipping ", transactionId);
            }
            // generate 6 digit OTP
            String otp = String.format("%06d", (int) (Math.random() * 900000) + 100000);

            //Store OTP in redis - expires in 5 minutes
            String otpkey= "verification:otp"+transactionId;
            redisTemplate.opsForValue().set(otpkey, otp,OTP_EXPIRAY_MINUTES, TimeUnit.MINUTES);

            //update status
            transaction.setStatus(TransactionStatuses.PENDING_VERIFICATION);
            transactionRepository.save(transaction);
            log.info("OTP generated for transcation: {} expired in {} min", transactionId, OTP_EXPIRAY_MINUTES);

            // notify users
            Map<String , Object> otpEvent = new HashMap<>();
            otpEvent.put("transactionId", transactionId);
            otpEvent.put("accountNumber", accountNumber);
            otpEvent.put("reason", reason);
            otpEvent.put("otp", otp);
            otpEvent.put("amount",payload.get("amount"));

            kafkaTemplate.send(TRANSACTIONAL_OTP_GENERATED_TOPIC,transactionId,otpEvent);

        }catch (Exception e){

            log.error("Error handling verification required {}",e.getMessage());
        }

    }

    @KafkaListener(topics = "fraud.check.clean")
    public void consumeFraudCheckCleanResult(
            @Payload Map<String, Object> payload
    )
    {
        try{
            String transactionId = (String) payload.get("transactionId");
            transactionService.processCleanResult(transactionId);

        }catch (Exception e){
            log.error("ERROR PROCESSING FRAUD CHECK RESULT: {}",e.getMessage());
        }
    }
}
