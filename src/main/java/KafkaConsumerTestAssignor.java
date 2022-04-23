

public class KafkaConsumerTestAssignor {

    public static void main(String[] args) throws InterruptedException {
        Thread consumer = new Thread  (new ConsumerThread());
        Thread server = new Thread  (new ServerThread());
        consumer.start();
        server.start();
        consumer.join();

    }


}




