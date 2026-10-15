package com.banking.transaction_service.Controller;

import com.banking.transaction_service.DTO.TransactionResponse;
import com.banking.transaction_service.DTO.TransferRequest;
import com.banking.transaction_service.Service.TransactionService;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/transactions")
@Slf4j
public class TransactionController {

    private final TransactionService transactionService;

    public TransactionController(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @PostMapping("/transfer")
    public ResponseEntity<TransactionResponse> transfer(
            @Valid @RequestBody TransferRequest request
            ){
        return ResponseEntity.status(HttpStatus.OK).body(transactionService.transfer(request));
    }

    @GetMapping("/{id}")
    public ResponseEntity<TransactionResponse> getTransactionById(@PathVariable String id){

        return ResponseEntity.ok(transactionService.getTransactionById(id));
    }

    @GetMapping("/account/{accountNumber}")
    public ResponseEntity<List<TransactionResponse>> getTransactionHistory(@PathVariable String accountNumber){

        return ResponseEntity.ok(transactionService.getTransactionHistory(accountNumber));
    }

    public ResponseEntity<TransactionResponse> verifyOtp(@PathVariable String transactionId, @RequestParam String otp){

        log.info("OTP verification Request for Transaction : {}", transactionId);
        return ResponseEntity.ok(
                transactionService.verifyOtp(transactionId,otp)
        );
    }







}
