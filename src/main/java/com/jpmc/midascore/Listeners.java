package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class Listeners {
	private int counter = 0;
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "transaction-group")
    public void listen(Transaction transaction) {
    	if (counter < 4) {
            System.out.println("✅ Received Transaction #" + (counter + 1) + " - Amount: " + transaction.getAmount());
            counter++;
        }
}
}