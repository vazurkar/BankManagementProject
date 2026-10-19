package com.banking.notificationservice.Service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class NotificationService {

    @KafkaListener(topics = "transactional.otp.generated")
    public void consumeOtpGenerated(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = String.valueOf(payload.get("accountNumber"));
            String otp = String.valueOf(payload.get("otp"));
            String amount = String.valueOf(payload.get("amount"));
            String reason = String.valueOf(payload.get("reason"));

            sendAlert(accountNumber,
                    "TRANSACTION VERIFICATION REQUIRED",
                    String.format(
                            "Suspicious activity detected on your account. Reason: %s. "
                                    + "A transaction of %s is pending verification. "
                                    + "YOUR OTP IS %s. Valid for 5 minutes. "
                                    + "If this wasn't you, ignore this message.",
                            reason, amount, otp));
        } catch (Exception e) {
            log.error("Error sending OTP notification", e);
        }
    }

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String, Object> payload) {
        try {
            String senderAccount = String.valueOf(payload.get("senderAccountNumber"));
            String receiverAccount = String.valueOf(payload.get("receiverAccountNumber"));
            String amount = String.valueOf(payload.get("amount"));

            sendAlert(senderAccount, "DEBIT ALERT",
                    String.format("%s debited from account %s", amount, senderAccount));

            sendAlert(receiverAccount, "CREDIT ALERT",
                    String.format("%s credited to account %s", amount, receiverAccount));
        } catch (Exception e) {
            log.error("Error sending transaction notification", e);
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void fraudDetected(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = String.valueOf(payload.get("accountNumber"));
            String reason = String.valueOf(payload.get("reason"));

            sendAlert(accountNumber, "SUSPICIOUS ACTIVITY DETECTED",
                    String.format(
                            "Your account %s has been blocked. Reason: %s. "
                                    + "Please contact your bank immediately.",
                            accountNumber, reason));
        } catch (Exception e) {
            log.error("Error sending fraud alert notification", e);
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefunded(@Payload Map<String, Object> payload) {
        try {
            String senderAccount = String.valueOf(payload.get("senderAccountNumber"));
            String reason = String.valueOf(payload.get("reason"));
            String amount = String.valueOf(payload.get("amount"));

            sendAlert(senderAccount, "REFUND PROCESSING",
                    String.format(
                            "Your transaction of %s was cancelled. Reason: %s. "
                                    + "%s has been refunded to account %s.",
                            amount, reason, amount, senderAccount));
        } catch (Exception e) {
            log.error("Error sending refund notification", e);
        }
    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = String.valueOf(payload.get("accountNumber"));
            String amount = String.valueOf(payload.get("amount"));

            sendAlert(accountNumber, "PAYMENT SUCCESSFUL",
                    String.format("Payment of %s completed. Razorpay Id: %s",
                            amount, payload.get("razorpayPaymentId")));
        } catch (Exception e) {
            log.error("Error sending payment completed notification", e);
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = String.valueOf(payload.get("accountNumber"));
            String amount = String.valueOf(payload.get("amount"));

            sendAlert(accountNumber, "PAYMENT FAILED",
                    String.format("Your payment of %s could not be processed. "
                            + "Please try again or contact support.", amount));
        } catch (Exception e) {
            log.error("Error sending payment failed notification", e);
        }
    }

    private void sendAlert(String accountNumber, String subject, String message) {
        log.info("----------------------------------------------------------");
        log.info("Account: {}", accountNumber);
        log.info("Subject: {}", subject);
        log.info("Message: {}", message);
        log.info("----------------------------------------------------------");
    }
}