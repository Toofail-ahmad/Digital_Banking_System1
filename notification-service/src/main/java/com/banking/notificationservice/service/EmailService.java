package com.banking.notificationservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

/**
 * EmailService — wraps JavaMailSender.
 *
 * Set notification.email.enabled=true + SMTP env-vars to send real emails.
 * When disabled (default / local dev), alerts are only logged.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class EmailService {

    private final JavaMailSender mailSender;

    @Value("${notification.email.from:noreply@digitalbank.com}")
    private String fromAddress;

    @Value("${notification.email.enabled:false}")
    private boolean emailEnabled;

    /**
     * Send an email alert.
     *
     * @param to      recipient address  (use accountNumber as placeholder until
     *                a user-lookup service is wired in)
     * @param subject email subject line
     * @param body    plain-text body
     */
    public void send(String to, String subject, String body) {
        if (!emailEnabled) {
            log.info("[EMAIL DISABLED] To: {} | Subject: {} | Body: {}", to, subject, body);
            return;
        }

        try {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(fromAddress);
            message.setTo(to);
            message.setSubject(subject);
            message.setText(body);

            mailSender.send(message);
            log.info("Email sent successfully to: {} subject: {}", to, subject);

        } catch (Exception e) {
            log.error("Failed to send email to: {} subject: {} error: {}", to, subject, e.getMessage(), e);
        }
    }
}

