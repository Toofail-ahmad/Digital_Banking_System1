package com.banking.Accountservice.dto;

import com.banking.Accountservice.entity.AccountStatus;
import com.banking.Accountservice.entity.AccountType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AccountResponse {

    private String id;
    private String accountNumber;
    private String accountHolderName;
    private String email;
    private String phone;
    private AccountType accountType;
    private AccountStatus status;
    private BigDecimal balance;
    private BigDecimal dailyTransactionLimit;
    private LocalDateTime createdAt;

    // Backward-compatibility aliases
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAccountHolerName() {
        return accountHolderName;
    }

    public void setAccountHolerName(String accountHolerName) {
        this.accountHolderName = accountHolerName;
    }

    public BigDecimal getDailyTransectionLimit() {
        return dailyTransactionLimit;
    }

    public void setDailyTransectionLimit(BigDecimal dailyTransectionLimit) {
        this.dailyTransactionLimit = dailyTransectionLimit;
    }
}
