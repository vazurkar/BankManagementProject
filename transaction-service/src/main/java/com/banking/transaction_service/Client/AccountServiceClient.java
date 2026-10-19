package com.banking.transaction_service.Client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.math.BigDecimal;

@FeignClient(name = "account-service", url = "${account.service.url}")
public interface AccountServiceClient {

    @PutMapping("/account/{accountNumber}/deduct")
    String deductBalance(@PathVariable String accountNumber,
                         @RequestParam BigDecimal amount);

    @PutMapping("/account/{accountNumber}/credit")
    String creditBalance(@PathVariable String accountNumber, @RequestParam BigDecimal amount);
}
