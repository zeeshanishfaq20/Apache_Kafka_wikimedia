package io.conduktor.demos.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.OkHttpClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangeProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";

        // Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "wikimedia.recentchange";

        // Create event handler
        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

        // Build the HTTP client
        OkHttpClient client = new OkHttpClient.Builder()
                .readTimeout(Duration.ofMinutes(5))
                .build();

        // Parse URL using HttpUrl.parse()
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        if (url == null) {
            throw new IllegalArgumentException("Invalid URL: null returned by HttpUrl.parse");
        }

        // Create and start the event source
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));

        // Start the event source
        EventSource eventSource = builder.build();
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }
}