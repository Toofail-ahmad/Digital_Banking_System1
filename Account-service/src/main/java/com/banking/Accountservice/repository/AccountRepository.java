package com.banking.Accountservice.repository;

import com.banking.Accountservice.entity.Account;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface AccountRepository extends JpaRepository<Account,String> {
    boolean existsByEmail(String email);
    boolean existsByAccountNumber(String AccountNumber);
    Optional<Account> findByAccountNumber(String accountNumber);
}
