package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var dispatcher = new KafkaDispatcher()) {
            for (int i = 0; i < 10; i++) {
                // Estou simulando como se fosse o ID do usuário. Então como serão hash diferentes, a execução
                // vai ficar bem distribuida entre as partições.
                var key = UUID.randomUUID().toString();

                var value = key + ",4612,8921398";

                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Thank you for your order! We are processing it.";
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }

}
