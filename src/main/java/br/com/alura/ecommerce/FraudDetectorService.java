package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class FraudDetectorService {
    public static void main(String[] args) throws InterruptedException {
        var consumer = new KafkaConsumer<String, String>(properties());
        // Me inscrevo como um escutador. Eu passo uma lista por parâmetro, com o nome dos tópicos
        // que quero me inscrever. Apesar de passarmos uma lista, nos inscrevemos em somente um tópico,
        // pois se não, fica muito bagunçado.
        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");
                for (var record : records) {
                    System.out.println("------------------------------------");
                    System.out.println("Procecessing new order, checking for fraud");
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());
                    Thread.sleep(5000);
                }
                System.out.println("Order processed");
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
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
        // Posso configurar o nome para o consumer. Mas preciso garantir que esse nome seja único, e não se
        // repita entre as demais instâncias.
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, FraudDetectorService.class.getSimpleName() + UUID.randomUUID().toString());
        // Posso configurar quantas mensagens ele vai pegar em cada poll. Como quero diminuir o tempo de commit,
        // coloco para pegar uma mensagem por poll, pois assim a cada mensagem ele faz um commit, e evita
        // o problema de tentar fazer um commit quando o kafka fez um rebalanceamento. Essa é uma configuração
        // bastante utilizada, inclusive por empresas grandes.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }
}
