package com.banking.transaction_service.Repository;

import com.banking.transaction_service.Entity.Transaction;
import org.apache.commons.lang3.ClassUtils;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface  TransactionRepository extends JpaRepository<Transaction,String> {
}
