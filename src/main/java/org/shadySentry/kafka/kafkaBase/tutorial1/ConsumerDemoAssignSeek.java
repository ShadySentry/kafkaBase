package org.shadySentry.kafka.kafkaBase.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    private static final String bootstrapServers = "127.0.0.1:9092";
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
    static String topic = "first_topic";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);


        TopicPartition partitionToReadFrom  = new TopicPartition(topic,0);
        long offset = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        consumer.seek(partitionToReadFrom, offset);

        int numberOfMessagesToRead = 5;
        int readMessagesCount = 0;
        boolean keepOnReading = true;
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record:records){
                logger.info("key: "+record.key() + ", Value: "+record.value());
                logger.info("partition: "+record.partition() + ", Offset: "+record.offset());
                readMessagesCount++;
                if (readMessagesCount>=numberOfMessagesToRead){
                    keepOnReading=false;
                    break;
                }
            }
        }
        logger.info("exiting the application with total read messages " + readMessagesCount);
    }
}
