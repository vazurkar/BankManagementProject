package com.banking.account_service.Service;

import com.banking.account_service.DTO.AccountResponse;
import com.banking.account_service.DTO.CreateAccountRequest;
import com.banking.account_service.Entity.Account;
import com.banking.account_service.Entity.AccountStatus;
import com.banking.account_service.Entity.AccountType;
import com.banking.account_service.Repository.AccountRepository;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Random;
import java.util.random.RandomGenerator;

@Service
@Slf4j
public class AccountService {

    private final AccountRepository accountRepository;
    private final RandomGenerator random;
    public AccountService(AccountRepository accountRepository ,  RandomGenerator random) {
        this.accountRepository = accountRepository;
        this.random = random;
    }

    public AccountResponse createAccount(@Valid CreateAccountRequest newAccount) {
        log.info("Creating account for: {}", newAccount.getEmail());

        if(accountRepository.existsByEmail(newAccount.getEmail())){
            throw new RuntimeException("Account already exists for email: " + newAccount.getEmail());
        }

        Account account = new Account();
        account.setEmail(newAccount.getEmail());
        account.setAccountHolderName(newAccount.getAccountHolderName());
        account.setPhone(newAccount.getPhone());
        account.setAccountType(newAccount.getAccountType());
        account.setStatus(AccountStatus.ACTIVE);
        account.setBalance(newAccount.getInitialDeposite());
        account.setAccountNumber(generateAccountNumber());
        account.setDailyTransactionLimit(
                newAccount.getAccountType() == AccountType.SAVINGS
                ? new BigDecimal("100000")
                        : new BigDecimal("500000")

        );
        Account savedAccount = accountRepository.save(account);
        log.info("Account created: {}", savedAccount.getAccountNumber());
        return mapToResponse(savedAccount);

    }

    public AccountResponse getAccountsByAccountNumber(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for accountNumber: " + accountNumber));

        return mapToResponse(account);
    }

    public String getBalanceByAccountNumber(String accountNumber) {

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for accountNumber: " + accountNumber));

        return account.getBalance().toString();
    }

    // block account - called by fraud detection service called by kafka
    public void blockAccount(String accountNumber) {

        log.info("Blocking account for: {}", accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for accountNumber: " + accountNumber));
        account.setStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);
        log.info("Account blocked: {}", account.getAccountNumber());


    }

    //deduct balence from sender account and called by transaction service
    public void deductBalance(String accountNumber, BigDecimal amount) {

        log.info("Deducting balance{} for: {}", amount, accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for accountNumber: " + accountNumber));

        if(account.getStatus() != AccountStatus.ACTIVE) {
            throw new RuntimeException("Account not active for accountNumber: " + accountNumber);

        }

        if(account.getBalance().compareTo(amount) < 0) {
            throw new RuntimeException("Insufficient funds for account balance: " + amount);
        }

        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);
        log.info("Account deducted: {} for: {}",amount, account.getAccountNumber());
        log.info("New Balence : {}", account.getBalance());

    }

    //called by transaction service via kafka
    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("Crediting balance{} for: {}", amount, accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for accountNumber: " + accountNumber));

        if(account.getStatus() != AccountStatus.ACTIVE) {
            throw new RuntimeException("Account not active for accountNumber: " + accountNumber);
        }

        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);
        log.info("Balence credited: {} for: {}",amount, account.getAccountNumber());


    }

    //generate usinque 12 digit acc number
    private String generateAccountNumber() {

        String accountNumber;

        do{
            long number = random.nextLong(1_000_000_000_000L);
            accountNumber = String.format("%012d",number);
        }while(accountRepository.existsByAccountNumber(accountNumber));
            return   accountNumber;

    }


    private AccountResponse mapToResponse(Account account) {
        AccountResponse response = new AccountResponse();
        response.setId(account.getId());
        response.setAccountNumber(account.getAccountNumber());
        response.setAccountHolderName(account.getAccountHolderName());
        response.setPhone(account.getPhone());
        response.setAccountType(account.getAccountType());
        response.setBalance(account.getBalance());
        response.setDailyTransactionLimit(account.getDailyTransactionLimit());
        response.setEmail(account.getEmail());
        response.setStatus(account.getStatus());
        response.setCreatedAt(account.getCreatedAt());

        return response;
    }
}
