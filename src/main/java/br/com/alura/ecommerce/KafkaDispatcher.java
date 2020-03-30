package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaDispatcher implements Closeable {

    // O producer recebe uma chave da mensagem e o valor da mensagem.
    private final KafkaProducer<String, String> producer;

    KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

    void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, value);

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            }
            System.out.println("sucesso enviando " + data.topic() + "::: partition " + data.partition() + " offset " + data.offset());
        };

        // O send retorna um Future, então chamado o get para esperar e passamos
        // uma função de callback para sermos avisados quando a mensagem termianr de enviar.
        producer.send(record, callback).get();
    }

    private static Properties properties() {
        var properties = new Properties();
        // Configuro o servidor do Kakka.
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Perciso configurar as classes que vão serializar as chaves e as mensagens,
        // nesse caso, converter de strings para bytes.
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    @Override
    public void close() {
        producer.close();
    }
}
