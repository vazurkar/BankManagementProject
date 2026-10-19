package com.banking.transaction_service.Service;

import com.banking.transaction_service.Client.AccountServiceClient;
import com.banking.transaction_service.DTO.TransactionResponse;
import com.banking.transaction_service.DTO.TransferRequest;
import com.banking.transaction_service.Entity.Transaction;
import com.banking.transaction_service.Entity.TransactionStatuses;
import com.banking.transaction_service.Entity.TransactionType;
import com.banking.transaction_service.Event.TransactionCompletedEvent;
import com.banking.transaction_service.Event.TransactionInitiatedEvents;
import com.banking.transaction_service.Repository.TransactionRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";


    /**
     * SAGA STEP 1 : INITIATE TRANSFER
     * DEDUCT FROM SENDER VIA FEIGN
     * SAVES TRANSACTION AS PROCESSING
     * PUBLISH EVENT TO KAFKA FOR FRAUDE CHECK
     * RRETURNS
     * * @param request
     * @return
     */
    public TransactionResponse transfer(@Valid TransferRequest request) {
        log.info("SAGA START - Transfer request received  , Transfer: {} -> {} amount: {}",request.getSenderAccountNumber(),request.getReceiverAccountNumber(),request.getAmount());

        //SAGA STEP 1 deduct from sender
        accountServiceClient.deductBalance(request.getSenderAccountNumber(), request.getAmount());

        //step 2 SAVE TRNSACTION AS PROCESSING
        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setType(TransactionType.TRANSFER);
        transaction.setStatus(TransactionStatuses.PROCESSING);
        transaction.setDescription(request.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved as processing : {}",savedTransaction.getId());


        //PUBLISH EVENT TO KAFKA FOR FRAUDE CHECK
        TransactionInitiatedEvents events = new TransactionInitiatedEvents(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getDescription(),
                savedTransaction.getAmount()
        );
        log.info("PUBLISH EVENT TO KAFKA FOR FRAUDE CHECK");
        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC,savedTransaction.getId(), events);
        log.info("SAGA STEP 2 - Transaction initiated-event published: {}",savedTransaction.getId());
        return mapToResponse(savedTransaction);
    }

    public TransactionResponse getTransactionById(String id) {

        return mapToResponse(transactionRepository.findById(id).orElseThrow(()-> new RuntimeException("Transaction not found")));

    }


    public List<TransactionResponse> getTransactionHistory(String accountNumber) {

        return transactionRepository.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());

    }

    public TransactionResponse verifyOtp(String transactionId, String otp) {

        log.info("Otp verification for the transaction id : {}",transactionId);

        Transaction transaction = transactionRepository.findById(transactionId).
                orElseThrow(()-> new RuntimeException("Transaction not found"));

        String otpKey = "verification:otp"+ transactionId;
        String storedKey = redisTemplate.opsForValue().get(otpKey);

        if(storedKey == null){
            //OTP expired
            log.warn("OTP Expired for transaction: {}",transactionId);
            compansateTransaction(transaction, "OTP expired- transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }
        if(!storedKey.equals(otp)){
            log.warn("Wrong OTP - blocking account and refunding: {}",transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensate(transaction ,"wrong OTP enterted - transaction cancelled" +
                    "Account blocked for security reasons" );
            return mapToResponse(transaction);

        }
            log.info("OTP verification completed for transaction: {}",transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);
    }


    private TransactionResponse mapToResponse(Transaction transaction) {
        TransactionResponse transactionResponse = new TransactionResponse();
        transactionResponse.setId(transaction.getId());
        transactionResponse.setDescription(transaction.getDescription());
        transactionResponse.setAmount(transaction.getAmount());
        transactionResponse.setStatus(transaction.getStatus());
        transactionResponse.setType(transaction.getType());
        transactionResponse.setReferenceNumber(transaction.getReferenceNumber());
        transactionResponse.setSenderAccountNumber(transaction.getSenderAccountNumber());
        transactionResponse.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        return transactionResponse;
    }

    private void compansateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA COMPENATION REFUNDING {} amount: {}",transaction.getSenderAccountNumber(),transaction.getAmount());
        //CREDIT MONEY BACK TO SENDER SYNCHRONOUSLY
        accountServiceClient.creditBalance(
                transaction.getSenderAccountNumber(),transaction.getAmount()
        );

        transaction.setStatus(TransactionStatuses.FLAGGED);
        transaction.setFailureReason(reason + "SAGA Compensation exhecuted , amount refunded at"+ LocalDateTime.now());
        transactionRepository.save(transaction);

        //PUBLISH REFUND EVENT - NOTIFICATION SERVICE ALERT USER
        Map<String ,Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId",transaction.getId());
        refundEvent.put("amount",transaction.getAmount());
        refundEvent.put("senderAccountNumber",transaction.getSenderAccountNumber());
        refundEvent.put("reason",reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC,transaction.getId(),refundEvent);
        log.info("SAGA Compansation complete- {}",transaction.getId());


    }

    private void blockAccountAndCompensate(Transaction transaction, String reason) {

        //publish fraud.detected event -> account service will block account
        Map<String ,Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId",transaction.getId());
        fraudEvent.put("accountNumber",transaction.getSenderAccountNumber());
        fraudEvent.put("reason",reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC,transaction.getSenderAccountNumber(),fraudEvent);
        log.warn("fraud detected published -- account will be blocked , kindly contact to bank");

        //SAGA COMPANSATION - REFUND SENDER
        compansateTransaction(transaction,reason);

    }

    private void completeTransaction(Transaction transaction) {
        transaction.setStatus(TransactionStatuses.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent();
        completedEvent.setTransactionId(transaction.getId());
        completedEvent.setSenderAccountNumber(transaction.getSenderAccountNumber());
        completedEvent.setAmount(transaction.getAmount());
        completedEvent.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        completedEvent.setDescription(transaction.getDescription());

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC,transaction.getId(),completedEvent);
        log.info("SAGA COMPLETE - TRANSACTION {} COMPLETED", transaction.getId());

    }

    public void processCleanResult(String transactionId){

        Transaction transaction = transactionRepository.findById(transactionId).
                orElseThrow(()-> new RuntimeException("Transaction not found"));

        if(transaction.getStatus() != TransactionStatuses.PROCESSING){
            log.warn("Traaansaction not processing - skipping", transactionId);
            return;
        }
        completeTransaction(transaction);

    }
}
