package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService {
    public static void main(String[] args) throws InterruptedException {

        var consumer = new KafkaConsumer<String, String>(properties());
        // Me inscrevo como um escutador. Eu passo uma lista por parâmetro, com o nome dos tópicos
        // que quero me inscrever. Apesar de passarmos uma lista, nos inscrevemos em somente um tópico,
        // pois se não, fica muito bagunçado.
        consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL"));
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");
                for (var record : records) {
                    System.out.println("------------------------------------");
                    System.out.println("Send email");
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());
                    Thread.sleep(1000);
                }
                System.out.println("Email sent");
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Da mesma forma como configuramos os serializadores, temos que configurar os desserializadores.
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Eu também preciso dizer o grupo que o consumidor pertence, para pode controlar e garantir
        // que ele receba todas as mensagens. Nesse caso, usei o próprio nome da classe como o ID do grupo.
        // Se eu tenho somente um serviço com esse grupo, eu recebo todas as mensagens. Se tiver mais alguém
        // escutando com o mesmo grupo, as mensagens são distribuidas entre os dois serviços.
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());
        return properties;
    }
}
