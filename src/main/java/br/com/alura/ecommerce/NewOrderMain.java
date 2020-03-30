package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // O producer recebe uma chave da mensagem e o valor da mensagem.
        var producer = new KafkaProducer<String, String>(properties());

        // Estou simulando como se fosse o ID do usuário. Então como serão hash diferentes, a execução
        // vai ficar bem distribuida entre as partições.
        var key = UUID.randomUUID().toString();

        var value = key+",4612,8921398";

        var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", key, value);

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            }
            System.out.println("sucesso enviando " + data.topic() + "::: partition " + data.partition() + " offset " + data.offset());
        };

        var email = "Thank you for your order! We are processing it.";
        // Como os parâmetros são String, posso omitir o <String,String> pois o compilador
        // consegue inferir isso.
        var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);

        // O send retorna um Future, então chamado o get para esperar e passamos
        // uma função de callback para sermos avisados quando a mensagem termianr de enviar.
        producer.send(record, callback).get();
        producer.send(emailRecord, callback).get();
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
}
