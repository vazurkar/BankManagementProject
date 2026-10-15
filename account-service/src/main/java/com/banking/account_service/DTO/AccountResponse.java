package com.banking.account_service.DTO;

import com.banking.account_service.Entity.AccountStatus;
import com.banking.account_service.Entity.AccountType;
import jakarta.persistence.Column;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AccountResponse {
    private String id;


    private String accountNumber;


    private String accountHolderName;


    private String email;


    private String phone;


    private AccountType accountType;


    private AccountStatus status;


    private BigDecimal balance;


    private BigDecimal dailyTransactionLimit;


    private LocalDateTime createdAt;


}
