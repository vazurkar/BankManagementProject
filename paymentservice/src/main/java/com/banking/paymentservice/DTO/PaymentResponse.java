package com.banking.paymentservice.DTO;

import com.banking.paymentservice.Entity.PaymentStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentResponse
{

    private String paymentId;

    private String razorPayPaymentId;

    private BigDecimal amount;

    private String currency;

    private String status;

    private String razorPayKeyId;

}
