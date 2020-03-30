package br.com.alura.ecommerce;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Tenho que implementar o Serializer do kafka para converter qualquer tipo de objeto para um array de bytes.
 *
 * @param <T>
 */
public class GsonSerializer<T> implements Serializer<T> {

    private final Gson gson = new GsonBuilder().create();

    @Override
    public byte[] serialize(String topic, T object) {
        // Uso o gson para converter qualquer objeto para json, em string, e depois converto a tring em bytes.
        return gson.toJson(object).getBytes();
    }
}
