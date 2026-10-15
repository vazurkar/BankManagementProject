package com.banking.transaction_service.Event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionInitiatedEvents {

    private String transactionId;

    private String senderAccountNumber;
    private String receiverAccountNumber;
    private String description;
    private BigDecimal amount;
}
