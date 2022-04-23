import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RateServiceImpl extends RateServiceGrpc.RateServiceImplBase {
    private static final Logger log = LogManager.getLogger(RateServiceImpl.class);
    @Override
    public void consumptionRate(RateRequest request, StreamObserver<RateResponse> responseObserver) {
        log.info("received new rate request {}", request.getRaterequest());
        RateResponse rate = RateResponse.newBuilder()
                .setRate(ConsumerThread.maxConsumptionRatePerConsumer)
                        .build();

        responseObserver.onNext(rate);
        responseObserver.onCompleted();

    }
}
