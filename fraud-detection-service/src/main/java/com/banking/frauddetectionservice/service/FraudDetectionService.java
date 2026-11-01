package com.banking.frauddetectionservice.service;

import com.banking.frauddetectionservice.client.AccountServiceClient;
import com.banking.frauddetectionservice.model.FraudCheckResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
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

    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    @Value("${fraud.max-transactions-per-minute:5}")
    private int maxTransactionsPerMinute;

    @Value("${fraud.suspicious-amount-multiplier:5}")
    private double suspiciousAmountMultiplier;

    @Value("${fraud.max-balance-percentage:0.90}")
    private double maxBalancePercentage;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private static final String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";

    @KafkaListener(topics = TRANSACTION_INITIATED_TOPIC, groupId = "fraud-detection-service-group")
    public void checkTransaction(Map<String, Object> payload) {
        try {
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("senderAccountNumber");

            if (payload.get("amount") == null) {
                log.warn("Payload missing amount: {}", payload);
                return;
            }

            BigDecimal amount = new BigDecimal(payload.get("amount").toString());

            log.info("Checking transaction: {}, Account: {}, Amount: {}",
                    transactionId, accountNumber, amount);

            // Fetch real balance from Account Service
            BigDecimal senderBalance = BigDecimal.ZERO;
            try {
                senderBalance = accountServiceClient.getBalance(accountNumber);
                log.info("Sender balance for account {}: {}", accountNumber, senderBalance);
            } catch (Exception e) {
                log.warn("Could not fetch balance from Account Service for {}: {}", accountNumber, e.getMessage());
            }

            // Perform fraud checks
            FraudCheckResult result = performFraudChecks(accountNumber, amount, senderBalance);

            if (result.isFraud()) {
                log.warn("Suspicious activity detected - account: {} reason: {} requesting OTP verification",
                        accountNumber, result.getReason());

                Map<String, Object> response = new HashMap<>();
                response.put("transactionId", transactionId);
                response.put("senderAccountNumber", accountNumber);
                response.put("accountNumber", accountNumber);
                response.put("amount", amount);
                response.put("fraud", true);
                response.put("reason", result.getReason());
                response.put("otpRequired", true);

                kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, response);
                log.info("Published verification.required for transaction: {}", transactionId);
                return;
            }

            log.info("Transaction {} passed fraud checks", transactionId);

            Map<String, Object> cleanResponse = new HashMap<>();
            cleanResponse.put("transactionId", transactionId);
            cleanResponse.put("senderAccountNumber", accountNumber);
            cleanResponse.put("amount", amount);
            cleanResponse.put("fraud", false);
            cleanResponse.put("status", "CLEAN");

            kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC, transactionId, cleanResponse);
            log.info("Published fraud.check.clean for transaction: {}", transactionId);

        } catch (Exception e) {
            log.error("Error processing fraud check for payload {}: {}", payload, e.getMessage(), e);
        }
    }

    private FraudCheckResult performFraudChecks(
            String accountNumber,
            BigDecimal amount,
            BigDecimal senderBalance) {

        // Pattern 1: Velocity Check
        if (isVelocityExceeded(accountNumber)) {
            return new FraudCheckResult(
                    true,
                    "Too many transactions in 60 seconds - Velocity limit exceeded"
            );
        }

        // Pattern 2: Amount check
        if (isAmountSuspicious(accountNumber, amount)) {
            return new FraudCheckResult(
                    true,
                    "Unusual transaction amount - exceeds " + suspiciousAmountMultiplier + "x your average"
            );
        }

        // Pattern 3: Balance Check
        if (senderBalance != null && senderBalance.compareTo(BigDecimal.ZERO) > 0
                && isBalanceCheckFailed(senderBalance, amount)) {
            return new FraudCheckResult(
                    true,
                    "Transaction exceeds " + (int)(maxBalancePercentage * 100) + "% of account balance"
            );
        }

        // Default response if all fraud checks pass
        return new FraudCheckResult(false, "Approved");
    }

    private boolean isVelocityExceeded(String accountNumber) {
        String key = "fraud:velocity:" + accountNumber;
        Long count = redisTemplate.opsForValue().increment(key);

        if (count != null && count == 1) {
            redisTemplate.expire(key, 60, TimeUnit.SECONDS);
        } else {
            Long ttl = redisTemplate.getExpire(key);
            if (ttl != null && ttl < 0) {
                redisTemplate.expire(key, 60, TimeUnit.SECONDS);
            }
        }

        log.info("Velocity check - account: {} count: {}/{}",
                accountNumber, count, maxTransactionsPerMinute);

        return count != null && count > maxTransactionsPerMinute;
    }

    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount) {
        String avgKey = "fraud:avg_amount:" + accountNumber;
        String avgStr = redisTemplate.opsForValue().get(avgKey);

        if (avgStr == null) {
            redisTemplate.opsForValue().set(avgKey, amount.toString());
            return false;
        }

        BigDecimal avgAmount = new BigDecimal(avgStr);
        BigDecimal threshold = avgAmount.multiply(
                BigDecimal.valueOf(suspiciousAmountMultiplier));

        boolean suspicious = amount.compareTo(threshold) > 0;

        log.info("Amount check - amount: {} threshold: {} suspicious: {}",
                amount, threshold, suspicious);

        // Only update running average if transaction is normal (prevent poison attacks)
        if (!suspicious) {
            BigDecimal newAvg = avgAmount.add(amount)
                    .divide(BigDecimal.valueOf(2), 2, RoundingMode.HALF_UP);
            redisTemplate.opsForValue().set(avgKey, newAvg.toString());
        }

        return suspicious;
    }

    private boolean isBalanceCheckFailed(BigDecimal senderBalance, BigDecimal amount) {
        BigDecimal maxAllowed = senderBalance.multiply(
                BigDecimal.valueOf(maxBalancePercentage));

        log.info("Balance check - amount: {} maxAllowed: {} suspicious: {}",
                amount, maxAllowed, amount.compareTo(maxAllowed) > 0);

        return amount.compareTo(maxAllowed) > 0;
    }
}
