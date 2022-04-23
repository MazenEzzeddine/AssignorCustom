import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class ServerThread  implements Runnable{
    private static final Logger log = LogManager.getLogger(ServerThread.class);

    @Override
    public void run() {
        Server server = ServerBuilder.forPort(5002).addService(ProtoReflectionService.newInstance())
                .addService(new RateServiceImpl()).build();
        try {
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }

        log.info("grpc server started at port 5002");

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            System.out.println("Shutting Down");
            server.shutdown();

        }));

        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}