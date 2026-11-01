package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    private static final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";
    private static final String OTP_GENERATED_TOPIC = "transaction.otp.generated";

    /**
     * SAGA STEP 1: Initiate transfer
     * 1. Creates transaction record in DB as PROCESSING
     * 2. Deducts from sender via Feign client
     * 3. Publishes event to Kafka for fraud evaluation
     */
    public TransactionResponse transfer(TransferRequest request) {
        log.info("SAGA START - Transfer: {} -> {} amount: {}",
                request.getSenderAccountNumber(),
                request.getReceiverAccountNumber(),
                request.getAmount());

        // Initialize and persist transaction record first
        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setType(TransactionType.TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setDescription(request.getDescription());
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved as PROCESSING: {}", savedTransaction.getId());

        // SAGA Step 1: Deduct from sender
        try {
            accountServiceClient.deductBalance(
                    request.getSenderAccountNumber(),
                    request.getAmount());
        } catch (Exception e) {
            log.error("Failed to deduct balance for transaction {}: {}", savedTransaction.getId(), e.getMessage());
            savedTransaction.setStatus(TransactionStatus.FAILED);
            savedTransaction.setFailureReason("Deduction failed: " + e.getMessage());
            transactionRepository.save(savedTransaction);
            throw new RuntimeException("Fund transfer initiation failed: " + e.getMessage(), e);
        }

        // SAGA Step 2: Publish for fraud check
        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, savedTransaction.getId(), event);
        log.info("SAGA STEP 2 - TransactionInitiatedEvent published: {}", savedTransaction.getId());

        return mapToResponse(savedTransaction);
    }

    public TransactionResponse getTransaction(String transactionId) {
        return mapToResponse(transactionRepository
                .findById(transactionId)
                .orElseThrow(() -> new RuntimeException(
                        "Transaction not found: " + transactionId
                )));
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        return transactionRepository
                .findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    /**
     * Kafka Listener for clean fraud check results
     */
    @KafkaListener(topics = "fraud.check.clean", groupId = "transaction-service-group")
    public void consumeCleanResult(Map<String, Object> payload) {
        try {
            String transactionId = (String) payload.get("transactionId");
            log.info("Received fraud.check.clean for transaction: {}", transactionId);
            if (transactionId != null) {
                processCleanResult(transactionId);
            }
        } catch (Exception e) {
            log.error("Error processing clean fraud check result: {}", e.getMessage(), e);
        }
    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException(
                        "Transaction not found: " + transactionId
                ));

        String otpKey = "verification:otp:" + transactionId;
        String attemptsKey = "verification:attempts:" + transactionId;

        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if (storedOtp == null) {
            log.warn("OTP expired for transaction: {}", transactionId);
            compensateTransaction(transaction, "OTP expired - transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }

        if (!storedOtp.equals(otp)) {
            Long attempts = redisTemplate.opsForValue().increment(attemptsKey);
            if (attempts != null && attempts == 1) {
                redisTemplate.expire(attemptsKey, 5, TimeUnit.MINUTES);
            }

            if (attempts != null && attempts >= 3) {
                log.warn("Max OTP attempts exceeded - blocking account and refunding: {}", transactionId);
                redisTemplate.delete(otpKey);
                redisTemplate.delete(attemptsKey);
                blockAccountAndCompensate(
                        transaction,
                        "Max OTP attempts exceeded - transaction cancelled, account blocked for security"
                );
                return mapToResponse(transaction);
            }

            log.warn("Wrong OTP entered for transaction: {} (attempt {}/3)", transactionId, attempts);
            throw new RuntimeException("Invalid OTP. " + (3 - (attempts != null ? attempts : 1)) + " attempt(s) remaining.");
        }

        // OTP correct - complete transaction
        log.info("OTP verified successfully - completing transaction: {}", transactionId);
        redisTemplate.delete(otpKey);
        redisTemplate.delete(attemptsKey);
        completeTransaction(transaction);
        return mapToResponse(transaction);
    }

    public void processCleanResult(String transactionId) {
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException(
                        "Transaction not found: " + transactionId
                ));

        if (transaction.getStatus() != TransactionStatus.PROCESSING) {
            log.warn("Transaction {} not in PROCESSING status ({}) - skipping completion",
                    transactionId, transaction.getStatus());
            return;
        }

        completeTransaction(transaction);
    }

    /**
     * SAGA Final Step: Complete transaction
     * 1. Credits receiver account via Feign
     * 2. Marks status as COMPLETED
     * 3. Publishes transaction.completed event
     */
    private void completeTransaction(Transaction transaction) {
        try {
            // SAGA Step: Credit receiver
            accountServiceClient.creditBalance(
                    transaction.getReceiverAccountNumber(),
                    transaction.getAmount());
            log.info("Credited {} to receiver account {}", transaction.getAmount(), transaction.getReceiverAccountNumber());

            transaction.setStatus(TransactionStatus.COMPLETED);
            transaction.setCompletedAt(LocalDateTime.now());
            transactionRepository.save(transaction);

            TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(
                    transaction.getId(),
                    transaction.getSenderAccountNumber(),
                    transaction.getReceiverAccountNumber(),
                    transaction.getAmount(),
                    transaction.getDescription()
            );

            kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), completedEvent);
            log.info("SAGA COMPLETE - Transaction {} successfully completed", transaction.getId());

        } catch (Exception e) {
            log.error("Failed to credit receiver for transaction {}: {}. Compensating sender...",
                    transaction.getId(), e.getMessage());
            compensateTransaction(transaction, "Failed to credit receiver: " + e.getMessage());
        }
    }

    /**
     * SAGA Compensation: Refund sender
     */
    private void compensateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA COMPENSATION - refunding: {} amount: {}",
                transaction.getSenderAccountNumber(),
                transaction.getAmount());

        try {
            accountServiceClient.creditBalance(
                    transaction.getSenderAccountNumber(),
                    transaction.getAmount());
        } catch (Exception e) {
            log.error("CRITICAL: Failed to refund sender account {}: {}",
                    transaction.getSenderAccountNumber(), e.getMessage());
        }

        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason +
                " - SAGA Compensation executed, amount refunded at " + LocalDateTime.now());

        transactionRepository.save(transaction);

        // Publish refund event for Notification Service
        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);
        log.info("SAGA COMPENSATION COMPLETE - {} refunded to {}",
                transaction.getAmount(), transaction.getSenderAccountNumber());
    }

    /**
     * SAGA Fraud Handling: Block account and refund sender
     */
    private void blockAccountAndCompensate(Transaction transaction, String reason) {
        Map<String, Object> fraudEvent = new HashMap<>();
        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
        fraudEvent.put("amount", transaction.getAmount());
        fraudEvent.put("reason", reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getSenderAccountNumber(), fraudEvent);
        log.warn("fraud.detected published - account: {} will be blocked",
                transaction.getSenderAccountNumber());

        compensateTransaction(transaction, reason);
    }

    private TransactionResponse mapToResponse(Transaction transaction) {
        TransactionResponse response = new TransactionResponse();
        response.setId(transaction.getId());
        response.setSenderAccountNumber(transaction.getSenderAccountNumber());
        response.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        response.setAmount(transaction.getAmount());
        response.setType(transaction.getType());
        response.setStatus(transaction.getStatus());
        response.setDescription(transaction.getDescription());
        response.setReferenceNumber(transaction.getReferenceNumber());
        response.setFailureReason(transaction.getFailureReason());
        response.setCreatedAt(transaction.getCreatedAt());
        response.setCompletedAt(transaction.getCompletedAt());

        return response;
    }
}
