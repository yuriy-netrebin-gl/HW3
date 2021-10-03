import lombok.Cleanup;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class KafkaConsumerTopTenBitcoin {
    private final static String TOPIC = "readBitcoinsTransactions";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaLabGroup" + new Date().getTime());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    static void runConsumer() {
        @Cleanup final Consumer<Long, String> consumer = createConsumer();

        final int giveUp = 100;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            List<JSONObject> list = new ArrayList<>();
            consumerRecords.forEach(record -> list.add(new JSONObject(record.value())));
            System.out.println("***** Top 10 bitcoin transactions based on price field (descending) *****");
            List<JSONObject> filteredList = list.stream()
                    .filter(r -> !r.isEmpty() && !r.isNull("data") && !r.getJSONObject("data").isEmpty())
                    .sorted(Comparator.comparing(r -> r.getJSONObject("data").getDouble("price")))
                    .collect(Collectors.toList());
            Collections.reverse(filteredList);
            AtomicInteger i = new AtomicInteger(1);
            filteredList.stream()
                    .limit(10)
                    .forEach(r -> System.out.printf("%d. %s%n", i.getAndIncrement(), r));
            consumer.commitAsync();
        }
        System.out.println("DONE");
    }

    public static void main(String[] args) {
        runConsumer();
    }
}
