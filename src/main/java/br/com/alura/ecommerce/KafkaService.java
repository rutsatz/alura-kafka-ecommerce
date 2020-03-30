package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> type) {
        this(parse, groupId, type);

        // Me inscrevo como um escutador. Eu passo uma lista por parâmetro, com o nome dos tópicos
        // que quero me inscrever. Apesar de passarmos uma lista, nos inscrevemos em somente um tópico,
        // pois se não, fica muito bagunçado.
        this.consumer.subscribe(Collections.singletonList(topic));

    }

    /**
     * Como temos o LogService que usa um Regex para assinar todas as filas, preciso desse segundo construtor que
     * recebe esse Pattern.
     *
     * @param groupId
     * @param topic
     * @param parse
     */
    public KafkaService(String groupId, Pattern topic, ConsumerFunction parse, Class<T> type) {
        this(parse, groupId,type);

        // Me inscrevo como um escutador. Eu passo uma lista por parâmetro, com o nome dos tópicos
        // que quero me inscrever. Apesar de passarmos uma lista, nos inscrevemos em somente um tópico,
        // pois se não, fica muito bagunçado.
        this.consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction parse, String groupId, Class<T> type) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(type,groupId));
    }

    void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");
                for (var record : records) {
                    parse.consume(record);
                }
            }
        }
    }

    private Properties properties(Class<T> type, String groupId) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Da mesma forma como configuramos os serializadores, temos que configurar os desserializadores.
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        // Eu também preciso dizer o grupo que o consumidor pertence, para pode controlar e garantir
        // que ele receba todas as mensagens. Nesse caso, usei o próprio nome da classe como o ID do grupo.
        // Se eu tenho somente um serviço com esse grupo, eu recebo todas as mensagens. Se tiver mais alguém
        // escutando com o mesmo grupo, as mensagens são distribuidas entre os dois serviços.
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // Posso configurar o nome para o consumer. Mas preciso garantir que esse nome seja único, e não se
        // repita entre as demais instâncias.
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        // Posso configurar quantas mensagens ele vai pegar em cada poll. Como quero diminuir o tempo de commit,
        // coloco para pegar uma mensagem por poll, pois assim a cada mensagem ele faz um commit, e evita
        // o problema de tentar fazer um commit quando o kafka fez um rebalanceamento. Essa é uma configuração
        // bastante utilizada, inclusive por empresas grandes.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        // Configuro o desserializador, dizendo qual a classe. Essa conf é recuperada na hora de montar o
        // GsonDesserializer.
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
