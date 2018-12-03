package demo.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
       new ConsumerDemoWithThreads().run();
    }
    private void run(){
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);
        String bootstrapServer = "localhost:9092";
        String groupId = "mi-sixth-application";
        String topic = "first_topic";
        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        //create the consumer runnable
        logger.info("Creating the consumer");
        Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServer,topic,groupId,latch);
        //start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread (() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("application has exited");
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Aplication got interrupted ",e);
        }finally {
            logger.info("application is closing");
        }
    }
    public class ConsumerRunnable implements Runnable{
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        public ConsumerRunnable(String bootstrapServer, String topic, String groupId, CountDownLatch latch){
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            //poll
            try{
                while(true){
                    ConsumerRecords<String,String> records =  consumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String, String> record : records){
                        logger.info("Key :"+record.key()+", Value: "+record.value());
                        logger.info("Partition: "+record.partition()+" ,Offset: "+record.offset());
                    }


                }
            }catch(WakeupException e){
                logger.info("Received shutdown signal");
            }finally{
                consumer.close();
                latch.countDown();//tell our main code we're done
            }

        }

        public void shutdown(){
            consumer.wakeup();//interrup consumer poll, it will thorw WakeUpException
        }
    }
}
