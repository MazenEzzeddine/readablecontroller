import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class QueryRate {

    private static final Logger log = LogManager.getLogger(QueryRate.class);
    static  String  BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
    static Properties props = new Properties();
    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;

         static AdminClient admin;


         static {
             props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
             admin = AdminClient.create(props);
         }


     static void queryConsumerGroup() throws ExecutionException, InterruptedException {

        DescribeConsumerGroupsResult describeConsumerGroupsResult =
                admin.describeConsumerGroups(Collections.singletonList("testgroup5"));
        KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                describeConsumerGroupsResult.all();

        consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();

        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get("testgroup5").members()) {
            log.info("Calling the consumer {} for its consumption rate ", memberDescription.host());
         callForConsumptionRate(memberDescription.host());
        }

        log.info("==================================");


    }


     static void callForConsumptionRate(String host) {

                ManagedChannel managedChannel = ManagedChannelBuilder.forAddress(host.substring(1), 5002)
                .usePlaintext()
                .build();
        RateServiceGrpc.RateServiceBlockingStub rateServiceBlockingStub
                = RateServiceGrpc.newBlockingStub(managedChannel);
        RateRequest rateRequest = RateRequest.newBuilder().setRate("Give me your rate")
                .build();
        log.info("connected to server {}", host);
        RateResponse rateResponse = rateServiceBlockingStub.consumptionRate(rateRequest);
        log.info("Received response on the rate: " + String.format("%.2f",rateResponse.getRate()));
        managedChannel.shutdown();
    }


}
