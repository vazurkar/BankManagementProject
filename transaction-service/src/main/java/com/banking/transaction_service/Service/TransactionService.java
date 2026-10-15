package com.banking.transaction_service.Service;

import com.banking.transaction_service.Client.AccountServiceClient;
import com.banking.transaction_service.DTO.TransactionResponse;
import com.banking.transaction_service.DTO.TransferRequest;
import com.banking.transaction_service.Entity.Transaction;
import com.banking.transaction_service.Entity.TransactionStatuses;
import com.banking.transaction_service.Entity.TransactionType;
import com.banking.transaction_service.Event.TransactionInitiatedEvents;
import com.banking.transaction_service.Repository.TransactionRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";

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
        log.info("Transfer request received  , Transfer: {} -> {} amount: {}",request.getSenderAccountNumber(),request.getReceiverAccountNumber(),request.getAmount());

        //SAGA STEP 1
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
    }


    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
    }

    public TransactionResponse verifyOtp(String transactionId, String otp) {
    }


    private TransactionResponse mapToResponse(Transaction transaction) {
        TransactionResponse transactionResponse = new TransactionResponse();
        transactionResponse.setId(transaction.getId());
        transactionResponse.setDescription(transaction.getDescription());
        transactionResponse.setAmount(transaction.getAmount());
        transactionResponse.setStatus(transaction.getStatus());
        transactionResponse.setReferenceNumber(transaction.getReferenceNumber());
        transactionResponse.setSenderAccountNumber(transaction.getSenderAccountNumber());
        transactionResponse.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        return transactionResponse;
    }
}
