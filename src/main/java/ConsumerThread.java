import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerThread implements Runnable {
    private static final Logger log = LogManager.getLogger(KafkaConsumerTestAssignor.class);
    public static KafkaConsumer<String, Customer> consumer = null;
    static float maxConsumptionRatePerConsumer = 0.0f;
    static float ConsumptionRatePerConsumerInThisPoll = 0.0f;
    static float averageRatePerConsumerForGrpc = 0.0f;

    static long pollsSoFar = 0;


    static Double maxConsumptionRatePerConsumer1 = 0.0d;
    //keep track of each of the vent processing latency for each event
    //index 0 < 1, index 1, < 2   and so on
    Long[] waitingTimes = new Long[10];

    @Override
    public void run() {
        KafkaConsumerConfig config = KafkaConsumerConfig.fromEnv();
        log.info(KafkaConsumerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaConsumerConfig.createProperties(config);
        int receivedMsgs = 0;
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, BinPackPartitionAssignor.class.getName());
        //props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        // props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
        boolean commit = !Boolean.parseBoolean(config.getEnableAutoCommit());
        consumer = new KafkaConsumer<String, Customer>(props);
        consumer.subscribe(Collections.singletonList(config.getTopic()));
        log.info("Subscribed to topic {}", config.getTopic());

        int warmup = 0;

        while (true) {
            Long timeBeforePolling = System.currentTimeMillis();
            //ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(0));
            if (records.count() != 0) {

                for (ConsumerRecord<String, Customer> record : records) {
                    log.info("Received message:");
                    log.info("\tpartition: {}", record.partition());
                    log.info("\toffset: {}", record.offset());
                    log.info("\tvalue: {}", record.value().toString());
                    if (record.headers() != null) {
                        log.info("\theaders: ");
                        for (Header header : record.headers()) {
                            log.info("\t\tkey: {}, value: {}", header.key(), new String(header.value()));
                        }
                    }
                    try {
                        Thread.sleep(Long.parseLong(config.getSleep()));
                        log.info("Sleeping for {}", config.getSleep());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                //getProcessingLatencyForEachEvent(records);
                if (commit) {
                    consumer.commitSync();
                }
                log.info("In this poll, received {} events", records.count());
                Long timeAfterPollingProcessingAndCommit = System.currentTimeMillis();
                ConsumptionRatePerConsumerInThisPoll = ((float) records.count() /
                        (float) (timeAfterPollingProcessingAndCommit - timeBeforePolling)) * 1000.0f;



              //if (warmup > 10) {
                 pollsSoFar += 1;
                  averageRatePerConsumerForGrpc = averageRatePerConsumerForGrpc +
                          (ConsumptionRatePerConsumerInThisPoll - averageRatePerConsumerForGrpc) / (float) (pollsSoFar);

                  if (maxConsumptionRatePerConsumer < ConsumptionRatePerConsumerInThisPoll) {
                      maxConsumptionRatePerConsumer = ConsumptionRatePerConsumerInThisPoll;
                  }
            //  } else {
                 // warmup++;
              //}
                maxConsumptionRatePerConsumer1 = Double.parseDouble(String.valueOf(averageRatePerConsumerForGrpc));
                log.info("ConsumptionRatePerConsumerInThisPoll in this poll {}", ConsumptionRatePerConsumerInThisPoll);
                log.info("maxConsumptionRatePerConsumer {}", maxConsumptionRatePerConsumer);

                log.info("averageRatePerConsumerForGrpc  {}", averageRatePerConsumerForGrpc);
            }
        }
    }


    private void getProcessingLatencyForEachEvent(ConsumerRecords<String, Customer> records) {

    }
}
