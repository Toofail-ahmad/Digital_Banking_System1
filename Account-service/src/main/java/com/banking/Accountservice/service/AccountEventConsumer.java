package com.banking.Accountservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor

public class AccountEventConsumer {
    private final AccountService accountService;
    /**
     * consume trasantion.completed event from kafka
     * credit reciver account
     * @param payload
     */
    @KafkaListener(topics ="transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String,Object> payload){

        try{
            String receiverAccount=(String) payload.get("receiverAccountNumber");
            BigDecimal amount= new BigDecimal(payload.get("amount").toString());

            log.info("credting account : {} amount:{} ",receiverAccount,amount);
            accountService.creditBalance(receiverAccount,amount);

        }catch(Exception e){
                log.error("Erorr Creding account : {}",e.getMessage());
        }
    }

    /**
     * Consume fraud.detected event from kafka
     * Blocks the flagged account.
     * @param payload
     */
    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetected(
            @Payload Map<String, Object> payload){

        try{

            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud detected - blocking account: {}", accountNumber);

            accountService.blockAccount(accountNumber);

        }
        catch(Exception e){
            log.error("Error blocking account: {}", e.getMessage());
        }
    }

}
