package com.banking.account_service.Controller;

import com.banking.account_service.DTO.AccountResponse;
import com.banking.account_service.DTO.CreateAccountRequest;
import com.banking.account_service.Entity.Account;
import com.banking.account_service.Repository.AccountRepository;
import com.banking.account_service.Service.AccountService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.coyote.Response;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/account")
@Slf4j
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;


    @PostMapping("/create")
    public ResponseEntity<AccountResponse> createAccount(@Valid @RequestBody CreateAccountRequest
            newAccount) {
        AccountResponse account = accountService.createAccount(newAccount);
            return new ResponseEntity<>(account, HttpStatus.CREATED);
    }

    @GetMapping("/{accountnumber}")
    public ResponseEntity<AccountResponse> getAccountsByAccountNumber(@PathVariable String accountNumber) {
        return ResponseEntity.ok(accountService.getAccountsByAccountNumber(accountNumber));
    }

    @GetMapping("/{accountnumber}/balance")
    public ResponseEntity<String> getBalanceByAccountNumber(@PathVariable String accountNumber) {
        return ResponseEntity.ok(accountService.getBalanceByAccountNumber(accountNumber));
    }

    @PutMapping("/{accountnumber}/block")
    public ResponseEntity<String> blockAccount(@PathVariable String accountNumber) {
        accountService.blockAccount(accountNumber);
        return ResponseEntity.ok("Account blocked successfully");
    }

    //SAGA STEP 1 - deduct balance
    //called by transaction service when transfer is initited

    @PutMapping("/{accountnumber}/deduct")
    public ResponseEntity<String>deductBalance(@PathVariable String accountNumber, @RequestParam BigDecimal amount) {

        accountService.deductBalance(accountNumber,amount);
        return ResponseEntity.ok("Amount deducted successfully");
    }

    // SAGA STEP 4 - Compansatng transaction endpoints
    //called by transaction service in 2 scinirios
    // 1- fraud detected -> refund sender (undo step 1)
    // 2- transaction completed -> credit receiver

    @PutMapping("/{accountnumber}/credit")
    public ResponseEntity<String>creditBalance(@PathVariable String accountNumber, @RequestParam BigDecimal amount) {

        accountService.creditBalance(accountNumber,amount);
        return ResponseEntity.ok("Amount credited successfully");
    }


}
