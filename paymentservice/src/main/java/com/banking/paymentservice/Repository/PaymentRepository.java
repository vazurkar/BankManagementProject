package com.banking.paymentservice.Repository;

import com.banking.paymentservice.Entity.Payment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, String> {

    Optional<Payment> findByRazorPayOrderId(String orderId);
}
