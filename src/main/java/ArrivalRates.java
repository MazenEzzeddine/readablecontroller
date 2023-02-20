import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ArrivalRates {


    private static final Logger log = LogManager.getLogger(ArrivalRates.class);
    static ArrayList<Partition> topicpartitions4 = new ArrayList<>();
    static ArrayList<Partition> topicpartitions3 = new ArrayList<>();


    static void arrivalRateTopic2() {
        HttpClient client = HttpClient.newHttpClient();

        List<URI> partitions2 = new ArrayList<>();
        try {
            partitions2 = Arrays.asList(
                    new URI(Constants.topic2p0),
                    new URI(Constants.topic2p1),
                    new URI(Constants.topic2p2),
                    new URI(Constants.topic2p3),
                    new URI(Constants.topic2p4)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag2 = new ArrayList<>();
        try {
            partitionslag2 = Arrays.asList(
                    new URI(Constants.topic2p0lag),
                    new URI(Constants.topic2p1lag),
                    new URI(Constants.topic2p2lag),
                    new URI(Constants.topic2p3lag),
                    new URI(Constants.topic2p4lag)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        ///////////////////////////////////////////////////

        List<CompletableFuture<String>> partitionsfutures2 = partitions2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        List<CompletableFuture<String>> partitionslagfuture2 = partitionslag2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partition2 = 0;
        double totalarrivalstopic2 = 0.0;
        double partitionArrivalRate2 = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures2) {
            try {
                partitionArrivalRate2 = Util.parseJsonArrivalRate(cf.get(), partition2);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            Scale2.topicpartitions2.get(partition2).setArrivalRate(partitionArrivalRate2);
            totalarrivalstopic2 += partitionArrivalRate2;
            partition2++;
        }
        log.info("totalArrivalRate for  topic 2 {}", totalarrivalstopic2);
        partition2 = 0;
        double totallag2 = 0.0;
        long partitionLag2 = 0L;
        for (CompletableFuture<String> cf : partitionslagfuture2) {
            try {
                partitionLag2 = Util.parseJsonArrivalLag(cf.get(), partition2).longValue();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            Scale2.topicpartitions2.get(partition2).setLag(partitionLag2);
            totallag2 += partitionLag2;
            partition2++;
        }
        log.info("totalLag for topic 2 {}", totallag2);


      /*  for (int i = 0; i <= 4; i++) {
            log.info("topic 2 partition {} has the following arrival rate {} and lag {}", i, Scale2p.topicpartitions2.get(i).getArrivalRate(),
                    Scale2p.topicpartitions2.get(i).getLag());
        }*/
        log.info("******************");
    }


    static void arrivalRateTopic1() {
        HttpClient client = HttpClient.newHttpClient();
        ////////////////////////////////////////////////////
        List<URI> partitions = new ArrayList<>();
        try {
            partitions = Arrays.asList(
                    new URI(Constants.topic1p0),
                    new URI(Constants.topic1p1),
                    new URI(Constants.topic1p2),
                    new URI(Constants.topic1p3),
                    new URI(Constants.topic1p4)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag = new ArrayList<>();
        try {
            partitionslag = Arrays.asList(
                    new URI(Constants.topic1p0lag),
                    new URI(Constants.topic1p1lag),
                    new URI(Constants.topic1p2lag),
                    new URI(Constants.topic1p3lag),
                    new URI(Constants.topic1p4lag)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        ///////////////////////////////////////////////////
        //launch queries for topic 1 lag and arrival get them from prometheus
        List<CompletableFuture<String>> partitionsfutures = partitions.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        List<CompletableFuture<String>> partitionslagfuture = partitionslag.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partition = 0;
        double totalarrivalstopic1 = 0.0;
        double partitionArrivalRate = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures) {
            try {
                partitionArrivalRate = Util.parseJsonArrivalRate(cf.get(), partition);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
          Scale1.topicpartitions1.get(partition).setArrivalRate(partitionArrivalRate);
              Scale2.topicpartitions2.get(partition).setArrivalRate(partitionArrivalRate*0.7);
            Scale5.topicpartitions5.get(partition).setArrivalRate(partitionArrivalRate*0.7);

           // Scale5p.topicpartitions5.get(partition).setArrivalRate(partitionArrivalRate*0.7);

            totalarrivalstopic1 += partitionArrivalRate;
            partition++;
        }
        log.info("totalArrivalRate for  topic 1 {}", totalarrivalstopic1);
        partition = 0;
        double totallag = 0.0;
        long partitionLag = 0L;
        for (CompletableFuture<String> cf : partitionslagfuture) {
            try {
                partitionLag = Util.parseJsonArrivalLag(cf.get(), partition).longValue();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            Scale1.topicpartitions1.get(partition).setLag(partitionLag);

            /*Scale2.topicpartitions2.get(partition).setLag((long)(partitionLag*0.7));
            Scale5.topicpartitions5.get(partition).setLag((long)(partitionLag*0.7));*/
             /* Scale2p.topicpartitions2.get(partition).setLag(0);
            Scale5p.topicpartitions5.get(partition).setLag(0);*/


            totallag += partitionLag;
            partition++;
        }

        double noise = Math.max(totalarrivalstopic1, Scale1.size*175 );
        double actualLag = Math.max(0, totallag - noise);

        log.info("totalLag for topic 1 {}", totallag);
        log.info("total actuaLag for topic 1 {}", actualLag);

        for (int i = 0; i <= 4; i++) {
            Scale2.topicpartitions2.get(i).setArrivalRate(Scale2.topicpartitions2.get(i).getArrivalRate() + ((actualLag*0.7) / (5*1.66)));
            Scale5.topicpartitions5.get(i).setArrivalRate(Scale5.topicpartitions5.get(i).getArrivalRate() + ((actualLag*0.7)/ (5*1.666)));
            Scale1.topicpartitions1.get(i).setLag((long)(actualLag / (5)));

            log.info( "Scale2.topicpartitions2.get(i) {} , {} ", i, Scale2.topicpartitions2.get(i).getArrivalRate());
            log.info( "Scale5.topicpartitions5.get(i) {} , {} ", i, Scale5.topicpartitions5.get(i).getArrivalRate());
            log.info( " Scale1.topicpartitions1.get(i).setLag  {} , {}", i, Scale1.topicpartitions1.get(i).getLag());


        }





      /*  for (int i = 0; i <= 4; i++) {
            log.info("partition {} for topic 1 has the following arrival rate {} and lag {}", i, Scalep.topicpartitions1.get(i).getArrivalRate(),
                    Scalep.topicpartitions1.get(i).getLag());
        }*/
        log.info("******************");
    }


    static void arrivalRateTopic4() {
        HttpClient client = HttpClient.newHttpClient();
        List<URI> partitions2 = new ArrayList<>();
        try {
            partitions2 = Arrays.asList(
                    new URI(Constants.topic4p0),
                    new URI(Constants.topic4p1),
                    new URI(Constants.topic4p2),
                    new URI(Constants.topic4p3),
                    new URI(Constants.topic4p4)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag2 = new ArrayList<>();
        try {
            partitionslag2 = Arrays.asList(
                    new URI(Constants.topic4p0lag),
                    new URI(Constants.topic4p1lag),
                    new URI(Constants.topic4p2lag),
                    new URI(Constants.topic4p3lag),
                    new URI(Constants.topic4p4lag)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        ///////////////////////////////////////////////////

        List<CompletableFuture<String>> partitionsfutures2 = partitions2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        List<CompletableFuture<String>> partitionslagfuture2 = partitionslag2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partition2 = 0;
        double totalarrivalstopic2 = 0.0;
        double partitionArrivalRate2 = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures2) {
            try {
                partitionArrivalRate2 = Util.parseJsonArrivalRate(cf.get(), partition2);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            topicpartitions4.get(partition2).setArrivalRate(partitionArrivalRate2);

            totalarrivalstopic2 += partitionArrivalRate2;
            partition2++;
        }
        log.info("totalArrivalRate for  topic 4 {}", totalarrivalstopic2);


        partition2 = 0;
        double totallag2 = 0.0;
        long partitionLag2 = 0L;

        for (CompletableFuture<String> cf : partitionslagfuture2) {
            try {
                partitionLag2 = Util.parseJsonArrivalLag(cf.get(), partition2).longValue();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            topicpartitions4.get(partition2).setLag(partitionLag2);
            totallag2 += partitionLag2;
            partition2++;
        }
        log.info("totalLag for topic 4 {}", totallag2);
    /*    for (int i = 0; i <= 4; i++) {
            log.info("topic 4 partition {} has the following arrival rate {} and lag {}", i, topicpartitions4.get(i).getArrivalRate(),
                    topicpartitions4.get(i).getLag());
        }*/
        log.info("******************");
    }



    static void arrivalRateTopic3() {
        HttpClient client = HttpClient.newHttpClient();
        List<URI> partitions2 = new ArrayList<>();
        try {
            partitions2 = Arrays.asList(
                    new URI(Constants.topic3p0),
                    new URI(Constants.topic3p1),
                    new URI(Constants.topic3p2),
                    new URI(Constants.topic3p3),
                    new URI(Constants.topic3p4)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag2 = new ArrayList<>();
        try {
            partitionslag2 = Arrays.asList(
                    new URI(Constants.topic3p0lag),
                    new URI(Constants.topic3p1lag),
                    new URI(Constants.topic3p2lag),
                    new URI(Constants.topic3p3lag),
                    new URI(Constants.topic3p4lag)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        ///////////////////////////////////////////////////

        List<CompletableFuture<String>> partitionsfutures2 = partitions2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());

        List<CompletableFuture<String>> partitionslagfuture2 = partitionslag2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());

        int partition2 = 0;
        double totalarrivalstopic2 = 0.0;
        double partitionArrivalRate2 = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures2) {
            try {
                partitionArrivalRate2 = Util.parseJsonArrivalRate(cf.get(), partition2);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            topicpartitions3.get(partition2).setArrivalRate(partitionArrivalRate2);
            totalarrivalstopic2 += partitionArrivalRate2;
            partition2++;
        }
        log.info("totalArrivalRate for  topic 3 {}", totalarrivalstopic2);
        partition2 = 0;
        double totallag2 = 0.0;
        long partitionLag2 = 0L;

        for (CompletableFuture<String> cf : partitionslagfuture2) {
            try {
                partitionLag2 = Util.parseJsonArrivalLag(cf.get(), partition2).longValue();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            topicpartitions3.get(partition2).setLag(partitionLag2);
            totallag2 += partitionLag2;
            partition2++;
        }

        log.info("totalLag for topic 3 {}", totallag2);
      /*  for (int i = 0; i <= 4; i++) {
            log.info("topic 3 partition {} has the following arrival rate {} and lag {}", i, topicpartitions3.get(i).getArrivalRate(),
                    topicpartitions3.get(i).getLag());
        }*/
        log.info("******************");

    }


    static void arrivalRateTopic5() {
        HttpClient client = HttpClient.newHttpClient();
        List<URI> partitions2 = new ArrayList<>();
        try {
            partitions2 = Arrays.asList(
                    new URI(Constants.topic5p0),
                    new URI(Constants.topic5p1),
                    new URI(Constants.topic5p2),
                    new URI(Constants.topic5p3),
                    new URI(Constants.topic5p4)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag2 = new ArrayList<>();
        try {
            partitionslag2 = Arrays.asList(
                    new URI(Constants.topic5p0lag),
                    new URI(Constants.topic5p1lag),
                    new URI(Constants.topic5p2lag),
                    new URI(Constants.topic5p3lag),
                    new URI(Constants.topic5p4lag)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        /////////////////////////////////////////////////////////////

        List<CompletableFuture<String>> partitionsfutures2 = partitions2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        List<CompletableFuture<String>> partitionslagfuture2 = partitionslag2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partition2 = 0;
        double totalarrivalstopic2 = 0.0;
        double partitionArrivalRate2 = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures2) {
            try {
                partitionArrivalRate2 = Util.parseJsonArrivalRate(cf.get(), partition2);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            Scale5.topicpartitions5.get(partition2).setArrivalRate(partitionArrivalRate2);

            totalarrivalstopic2 += partitionArrivalRate2;
            partition2++;
        }
        log.info("totalArrivalRate for  topic 5 {}", totalarrivalstopic2);


        partition2 = 0;
        double totallag2 = 0.0;
        long partitionLag2 = 0L;

        for (CompletableFuture<String> cf : partitionslagfuture2) {
            try {
                partitionLag2 = Util.parseJsonArrivalLag(cf.get(), partition2).longValue();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            Scale5.topicpartitions5.get(partition2).setLag(partitionLag2);
            totallag2 += partitionLag2;
            partition2++;
        }

        log.info("totalLag for topic 5 {}", totallag2);
     /*   for (int i = 0; i <= 4; i++) {
            log.info("topic 5 partition {} has the following arrival rate {} and lag {}", i, Scale5p.topicpartitions5.get(i).getArrivalRate(),
                    Scale5p.topicpartitions5.get(i).getLag());
        }*/

        log.info("******************");

    }



    static void arrivalRateTopic5Avg() {
        HttpClient client = HttpClient.newHttpClient();
        List<URI> partitions2 = new ArrayList<>();
        try {
            partitions2 = Arrays.asList(
                    new URI(Constants.topic5p0),
                    new URI(Constants.topic5p1),
                    new URI(Constants.topic5p2),
                    new URI(Constants.topic5p3),
                    new URI(Constants.topic5p4)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        List<URI> partitionslag2 = new ArrayList<>();
        try {
            partitionslag2 = Arrays.asList(
                    new URI(Constants.topic5p0lagavg),
                    new URI(Constants.topic5p1lagavg),
                    new URI(Constants.topic5p2lagavg),
                    new URI(Constants.topic5p3lagavg),
                    new URI(Constants.topic5p4lagavg)
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        /////////////////////////////////////////////////////////////

        List<CompletableFuture<String>> partitionsfutures2 = partitions2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        List<CompletableFuture<String>> partitionslagfuture2 = partitionslag2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partition2 = 0;
        double totalarrivalstopic2 = 0.0;
        double partitionArrivalRate2 = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures2) {
            try {
                partitionArrivalRate2 = Util.parseJsonArrivalRate(cf.get(), partition2);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            Scale5.topicpartitions5avg.get(partition2).setArrivalRate(partitionArrivalRate2);

            totalarrivalstopic2 += partitionArrivalRate2;
            partition2++;
        }
        log.info("totalArrivalRate for  topic 5 {}", totalarrivalstopic2);


        partition2 = 0;
        double totallag2 = 0.0;
        long partitionLag2 = 0L;

        for (CompletableFuture<String> cf : partitionslagfuture2) {
            try {
                partitionLag2 = Util.parseJsonArrivalLag(cf.get(), partition2).longValue();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            Scale5.topicpartitions5avg.get(partition2).setLag(partitionLag2);
            totallag2 += partitionLag2;
            partition2++;
        }

        log.info("totalLag for topic 5 {}", totallag2);
        for (int i = 0; i <= 4; i++) {
            log.info("topic 5 partition {} has the following arrival rate {} and average lag {}", i, Scale5.topicpartitions5avg.get(i).getArrivalRate(),
                    Scale5.topicpartitions5avg.get(i).getLag());
        }

        log.info("******************");

    }

}
