package com.jpmc.midascore.service;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Service
public class TransactionService {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate;

    public TransactionService(UserRepository userRepository,
                              TransactionRecordRepository transactionRecordRepository,
                              RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "transaction-group")
    public void processTransaction(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId()).orElse(null);
        UserRecord recipient = userRepository.findById(transaction.getRecipientId()).orElse(null);

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {
            TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());
            transactionRecordRepository.save(transactionRecord);

            sender.setBalance(sender.getBalance() - transaction.getAmount());

            float incentiveAmount = 0;
            try {
                incentiveAmount = getIncentiveApi(transaction).getAmount();
            } catch (Exception e) {
                System.out.println("Error occurred while calling Incentive API: " + e);
            }

            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + Math.max(0, incentiveAmount));

            userRepository.save(sender);
            userRepository.save(recipient);
        } else {
            System.out.println("Transaction discarded: " + transaction);
        }
    }

    public Incentive getIncentiveApi(Transaction transaction) {
        String url = "http://localhost:8080/incentive";
        return restTemplate.postForObject(url, transaction, Incentive.class);
    }

    public Balance getBalance(Long userId) {
        Optional<UserRecord> user = userRepository.findById(userId);
        return new Balance(user.map(UserRecord::getBalance).orElse(0f));
    }
}
