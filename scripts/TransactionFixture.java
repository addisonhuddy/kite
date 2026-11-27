import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

class TransactionFixture {
    public static void main(String[] args) throws Exception {
        String topic = args[0];
        String splitTopic = args[1];
        var config = Map.<String, Object>of("bootstrap.servers", "localhost:9092");
        try (var tx = new KafkaProducer<>(Map.<String, Object>of(
                 "bootstrap.servers", "localhost:9092",
                 "transactional.id", topic), new StringSerializer(), new StringSerializer());
             var plain = new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())) {
            tx.initTransactions();
            tx.beginTransaction();
            tx.send(new ProducerRecord<>(topic, 0, null, "aborted-first")).get();
            tx.send(new ProducerRecord<>(splitTopic, 0, null, "aborted-first")).get();
            tx.send(new ProducerRecord<>(splitTopic, 0, null, "aborted-middle")).get();
            tx.send(new ProducerRecord<>(splitTopic, 0, null, "aborted-tail")).get();
            tx.abortTransaction();
            tx.beginTransaction();
            tx.send(new ProducerRecord<>(topic, 0, null, "committed")).get();
            tx.send(new ProducerRecord<>(splitTopic, 0, null, "committed")).get();
            tx.commitTransaction();
            tx.beginTransaction();
            tx.send(new ProducerRecord<>(topic, 0, null, "aborted-last")).get();
            tx.abortTransaction();
            tx.beginTransaction();
            tx.send(new ProducerRecord<>(topic, 0, null, "open")).get();
            plain.send(new ProducerRecord<>(topic, 0, null, "after-open")).get();
            System.out.println("READY");
            if (System.in.read() == -1) throw new IllegalStateException("fixture closed before commit");
            tx.commitTransaction();
        }
    }
}
