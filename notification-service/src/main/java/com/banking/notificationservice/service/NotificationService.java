package com.banking.notificationservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationService {

    private final EmailService emailService;

    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOtpGenerated(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = String.valueOf(payload.getOrDefault("accountNumber", "N/A"));
            String otp = String.valueOf(payload.getOrDefault("otp", ""));
            String amount = String.valueOf(payload.getOrDefault("amount", "0"));
            String reason = String.valueOf(payload.getOrDefault("reason", "Suspicious activity"));

            String message = """
                    Suspicious activity detected on your account.
                    Reason: %s
                    A transaction of %s is pending verification.
                    Your OTP is: %s. Valid for 5 minutes.
                    If this wasn't you - ignore this message.
                    """.formatted(reason, amount, otp);

            sendAlert(accountNumber, "TRANSACTION VERIFICATION REQUIRED", message);

        } catch (Exception e) {
            log.error("Error sending OTP notification: {}", e.getMessage(), e);
        }
    }

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String, Object> payload) {
        try {
            String senderAccount = String.valueOf(payload.getOrDefault("senderAccountNumber", "N/A"));
            String receiverAccount = String.valueOf(payload.getOrDefault("receiverAccountNumber", "N/A"));
            String amount = String.valueOf(payload.getOrDefault("amount", "0"));

            // DEBIT ALERT
            sendAlert(senderAccount, "DEBIT ALERT",
                    "%s debited from account %s".formatted(amount, senderAccount));

            // CREDIT ALERT (Skip if sender == receiver)
            if (!senderAccount.equals(receiverAccount)) {
                sendAlert(receiverAccount, "CREDIT ALERT",
                        "%s credited to account %s".formatted(amount, receiverAccount));
            }

        } catch (Exception e) {
            log.error("Error processing transaction completed notification: {}", e.getMessage(), e);
        }
    }

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = String.valueOf(payload.getOrDefault("accountNumber", "N/A"));
            String reason = String.valueOf(payload.getOrDefault("reason", "Suspicious activity"));
            String transactionId = String.valueOf(payload.getOrDefault("transactionId", "N/A"));
            String amount = String.valueOf(payload.getOrDefault("amount", "N/A"));

            String message = """
                    Your account %s has been blocked.
                    Transaction ID: %s
                    Amount: %s
                    Reason: %s
                    Please contact your bank immediately.
                    """.formatted(accountNumber, transactionId, amount, reason);

            sendAlert(accountNumber, "SUSPICIOUS ACTIVITY DETECTED", message);
            log.info("Fraud alert sent to account: {} for reason: {}", accountNumber, reason);

        } catch (Exception e) {
            log.error("Error sending fraud alert: {}", e.getMessage(), e);
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeTransactionRefunded(@Payload Map<String, Object> payload) {
        try {
            String senderAccount = String.valueOf(payload.getOrDefault("senderAccountNumber", "N/A"));
            String amount = String.valueOf(payload.getOrDefault("amount", "0"));
            String reason = String.valueOf(payload.getOrDefault("reason", "Cancelled"));

            String message = """
                    Your transaction of %s was cancelled.
                    Reason: %s
                    %s has been refunded to account %s.
                    """.formatted(amount, reason, amount, senderAccount);

            sendAlert(senderAccount, "REFUND PROCESSED", message);

        } catch (Exception e) {
            log.error("Error sending refund alert: {}", e.getMessage(), e);
        }
    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = String.valueOf(payload.getOrDefault("accountNumber", "N/A"));
            String amount = String.valueOf(payload.getOrDefault("amount", "0"));
            String razorpayPaymentId = String.valueOf(payload.getOrDefault("razorpayPaymentId", "N/A"));

            String message = """
                    Payment of %s completed.
                    Razorpay ID: %s
                    """.formatted(amount, razorpayPaymentId);

            sendAlert(accountNumber, "PAYMENT SUCCESSFUL", message);

        } catch (Exception e) {
            log.error("Error sending payment notification: {}", e.getMessage(), e);
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = String.valueOf(payload.getOrDefault("accountNumber", "N/A"));
            String amount = String.valueOf(payload.getOrDefault("amount", "0"));

            String message = """
                    Your payment of %s could not be processed.
                    Please try again or contact support.
                    """.formatted(amount);

            sendAlert(accountNumber, "PAYMENT FAILED", message);

        } catch (Exception e) {
            log.error("Error sending payment failure notification: {}", e.getMessage(), e);
        }
    }

    /**
     * Dispatches an alert via email.
     *
     * NOTE: accountNumber is used as the "to" address placeholder.
     * Wire in a UserLookupService to resolve the real email address
     * once a user-management service is available.
     */
    private void sendAlert(String accountNumber, String subject, String message) {
        log.info("Alert dispatched | account: {} | subject: {}", accountNumber, subject);
        // accountNumber is used as recipient placeholder;
        // replace with real email lookup when available.
        emailService.send(accountNumber, subject, message);
    }
}
