package com.banking.fraud_detectionservice.Service;

import com.banking.fraud_detectionservice.Client.AccountServiceClient;
import com.banking.fraud_detectionservice.Model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionService {

    private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private static final String FRAUD_CHECK_CLEAN_TOPIC = "fraud.check.clean";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;
    private final AccountServiceClient accountServiceClient;

    @Value("${suspicious-Amount-Multipler}")
    private double suspiciousAmountMultipler;

    @Value("${max-transaction-per-minute}")
    private int maxTransactionPerMinute;

    @Value("${max-Balence-Percentage}")
    private double maxBalencePercentage;

    public void checkTransaction(Map<String, Object> payload) {
        String transactionId = (String) payload.get("transactionId");
        String accountNumber = (String) payload.get("senderAccountNumber");
        BigDecimal amount = new BigDecimal(payload.get("amount").toString());

        // The transaction service has already deducted the amount before publishing,
        // so this is the balance AFTER the transfer.
        String balanceStr = accountServiceClient.getBalanceByAccountNumber(accountNumber);
        BigDecimal balanceAfterDeduction = (balanceStr == null || balanceStr.isBlank())
                ? null
                : new BigDecimal(balanceStr.trim());
        BigDecimal balanceBeforeTransfer = balanceAfterDeduction == null
                ? null
                : balanceAfterDeduction.add(amount);

        log.info("Checking transaction: {} account: {} amount: {} balance before transfer: {}",
                transactionId, accountNumber, amount, balanceBeforeTransfer);

        FraudCheckResult result = performFraudChecks(accountNumber, amount, balanceBeforeTransfer);

        if (result.isFraud()) {
            log.info("Suspicious activity detected in account: {} reason: {} - requesting OTP verification",
                    accountNumber, result.getReason());

            Map<String, Object> verificationEvent = new HashMap<>();
            verificationEvent.put("transactionId", transactionId);
            verificationEvent.put("accountNumber", accountNumber);
            verificationEvent.put("amount", amount);
            verificationEvent.put("reason", result.getReason());
            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationEvent);
        } else {
            log.info("Transaction {} is clean, no fraud detected", transactionId);

            Map<String, Object> cleanEvent = new HashMap<>();
            cleanEvent.put("transactionId", transactionId);
            cleanEvent.put("isFraud", false);
            cleanEvent.put("reason", null);
            kafkaTemplate.send(FRAUD_CHECK_CLEAN_TOPIC, transactionId, cleanEvent);
        }
    }

    private FraudCheckResult performFraudChecks(String accountNumber, BigDecimal amount, BigDecimal balanceBeforeTransfer) {

        // velocity check
        if (isVelocityExceeded(accountNumber)) {
            return new FraudCheckResult(true, "Too many transactions in 60 seconds - velocity limit exceeded");
        }

        // amount check
        if (isAmountSuspicious(accountNumber, amount)) {
            return new FraudCheckResult(true, "Unusual transaction amount");
        }

        // balance check
        if (balanceBeforeTransfer != null
                && balanceBeforeTransfer.compareTo(BigDecimal.ZERO) > 0
                && isBalanceCheckFailed(balanceBeforeTransfer, amount)) {
            return new FraudCheckResult(true, "Transaction exceeds 90% of account balance");
        }

        return new FraudCheckResult(false, null);
    }

    private boolean isVelocityExceeded(String accountNumber) {
        String key = "fraud:velocity:" + accountNumber;
        Long count = redisTemplate.opsForValue().increment(key);

        if (count != null && count == 1) {
            redisTemplate.expire(key, 60, TimeUnit.SECONDS);
        }
        log.info("Velocity check - account: {} count: {}/{}", accountNumber, count, maxTransactionPerMinute);
        return count != null && count > maxTransactionPerMinute;
    }

    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount) {
        String avgKey = "fraud:avg_amount:" + accountNumber;
        String avgStr = redisTemplate.opsForValue().get(avgKey);

        // first transaction seen for this account: nothing to compare against yet
        if (avgStr == null) {
            redisTemplate.opsForValue().set(avgKey, amount.toPlainString());
            return false;
        }

        BigDecimal avgAmount = new BigDecimal(avgStr);
        BigDecimal threshold = avgAmount.multiply(BigDecimal.valueOf(suspiciousAmountMultipler));

        // update running average
        BigDecimal newAvg = avgAmount.add(amount)
                .divide(BigDecimal.valueOf(2), 2, RoundingMode.HALF_UP);
        redisTemplate.opsForValue().set(avgKey, newAvg.toPlainString());

        boolean suspicious = amount.compareTo(threshold) > 0;
        log.info("Amount check - amount: {} threshold: {} suspicious: {}", amount, threshold, suspicious);
        return suspicious;
    }

    private boolean isBalanceCheckFailed(BigDecimal balanceBeforeTransfer, BigDecimal amount) {
        BigDecimal maxAllowed = balanceBeforeTransfer.multiply(BigDecimal.valueOf(maxBalencePercentage));
        boolean suspicious = amount.compareTo(maxAllowed) > 0;
        log.info("Balance check - amount: {} maxAllowed: {} suspicious: {}", amount, maxAllowed, suspicious);
        return suspicious;
    }
}