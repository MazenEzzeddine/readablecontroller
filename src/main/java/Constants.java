public class Constants {


    //HttpClient client = HttpClient.newHttpClient();
    static String topic1ar = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";
    static String topic1p0 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%220%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic1p1 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%221%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic1p2 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%222%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic1p3 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%223%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic1p4 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic1%22,partition=%224%22,namespace=%22default%22%7D%5B20s%5D))";


    //  "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22, namespace=%22kubernetes_namespace%7D)%20by%20(consumergroup,topic)"
    //sum(kafka_consumergroup_lag{consumergroup=~"$consumergroup",topic=~"$topic", namespace=~"$kubernetes_namespace"}) by (consumergroup, topic)

    static String topic1lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,namespace=%22default%22%7D)%20by%20(consumergroup,topic)";
    static String topic1p0lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%220%22,namespace=%22default%22%7D";
    static String topic1p1lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%221%22,namespace=%22default%22%7D";
    static String topic1p2lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%222%22,namespace=%22default%22%7D";
    static String topic1p3lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%223%22,namespace=%22default%22%7D";
    static String topic1p4lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22,partition=%224%22,namespace=%22default%22%7D";


    ////////////////////////topic2


    static String topic2ar = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";
    static String topic2p0 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%220%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic2p1 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%221%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic2p2 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%222%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic2p3 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%223%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic2p4 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic2%22,partition=%224%22,namespace=%22default%22%7D%5B20s%5D))";


    //  "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22, namespace=%22kubernetes_namespace%7D)%20by%20(consumergroup,topic)"
    //sum(kafka_consumergroup_lag{consumergroup=~"$consumergroup",topic=~"$topic", namespace=~"$kubernetes_namespace"}) by (consumergroup, topic)

    static String topic2lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,namespace=%22default%22%7D)%20by%20(consumergroup,topic)";
    static String topic2p0lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%220%22,namespace=%22default%22%7D";
    static String topic2p1lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%221%22,namespace=%22default%22%7D";
    static String topic2p2lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%222%22,namespace=%22default%22%7D";
    static String topic2p3lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%223%22,namespace=%22default%22%7D";
    static String topic2p4lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup2%22,topic=%22testtopic2%22,partition=%224%22,namespace=%22default%22%7D";


    ////////////////////////topic4


    static String topic4ar = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic4%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";
    static String topic4p0 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic4%22,partition=%220%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic4p1 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic4%22,partition=%221%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic4p2 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic4%22,partition=%222%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic4p3 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic4%22,partition=%223%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic4p4 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic4%22,partition=%224%22,namespace=%22default%22%7D%5B20s%5D))";


    //  "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22, namespace=%22kubernetes_namespace%7D)%20by%20(consumergroup,topic)"
    //sum(kafka_consumergroup_lag{consumergroup=~"$consumergroup",topic=~"$topic", namespace=~"$kubernetes_namespace"}) by (consumergroup, topic)

    static String topic4lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup4%22,topic=%22testtopic4%22,namespace=%22default%22%7D)%20by%20(consumergroup,topic)";
    static String topic4p0lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup4%22,topic=%22testtopic4%22,partition=%220%22,namespace=%22default%22%7D";
    static String topic4p1lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup4%22,topic=%22testtopic4%22,partition=%221%22,namespace=%22default%22%7D";
    static String topic4p2lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup4%22,topic=%22testtopic4%22,partition=%222%22,namespace=%22default%22%7D";
    static String topic4p3lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup4%22,topic=%22testtopic4%22,partition=%223%22,namespace=%22default%22%7D";
    static String topic4p4lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup4%22,topic=%22testtopic4%22,partition=%224%22,namespace=%22default%22%7D";



    //////////////////////////////////////////////////////


    static String topic3ar = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic3%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";
    static String topic3p0 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic3%22,partition=%220%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic3p1 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic3%22,partition=%221%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic3p2 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic3%22,partition=%222%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic3p3 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic3%22,partition=%223%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic3p4 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic3%22,partition=%224%22,namespace=%22default%22%7D%5B20s%5D))";


    //  "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22, namespace=%22kubernetes_namespace%7D)%20by%20(consumergroup,topic)"
    //sum(kafka_consumergroup_lag{consumergroup=~"$consumergroup",topic=~"$topic", namespace=~"$kubernetes_namespace"}) by (consumergroup, topic)

    static String topic3lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup3%22,topic=%22testtopic3%22,namespace=%22default%22%7D)%20by%20(consumergroup,topic)";
    static String topic3p0lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup3%22,topic=%22testtopic3%22,partition=%220%22,namespace=%22default%22%7D";
    static String topic3p1lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup3%22,topic=%22testtopic3%22,partition=%221%22,namespace=%22default%22%7D";
    static String topic3p2lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup3%22,topic=%22testtopic3%22,partition=%222%22,namespace=%22default%22%7D";
    static String topic3p3lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup3%22,topic=%22testtopic3%22,partition=%223%22,namespace=%22default%22%7D";
    static String topic3p4lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup3%22,topic=%22testtopic3%22,partition=%224%22,namespace=%22default%22%7D";



    ////////////////////////////////////////////////////////////////



    static String topic5ar = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic5%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";
    static String topic5p0 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic5%22,partition=%220%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic5p1 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic5%22,partition=%221%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic5p2 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic5%22,partition=%222%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic5p3 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic5%22,partition=%223%22,namespace=%22default%22%7D%5B20s%5D))";
    static String topic5p4 = "http://prometheus-operated:9090/api/v1/query?" +
            "query=sum(rate(kafka_topic_partition_current_offset%7Btopic=%22testtopic5%22,partition=%224%22,namespace=%22default%22%7D%5B20s%5D))";




    //  "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup1%22,topic=%22testtopic1%22, namespace=%22kubernetes_namespace%7D)%20by%20(consumergroup,topic)"
    //sum(kafka_consumergroup_lag{consumergroup=~"$consumergroup",topic=~"$topic", namespace=~"$kubernetes_namespace"}) by (consumergroup, topic)

    static String topic5lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "sum(kafka_consumergroup_lag%7Bconsumergroup=%22testgroup5%22,topic=%22testtopic5%22,namespace=%22default%22%7D)%20by%20(consumergroup,topic)";
    static String topic5p0lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup5%22,topic=%22testtopic5%22,partition=%220%22,namespace=%22default%22%7D";
    static String topic5p1lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup5%22,topic=%22testtopic5%22,partition=%221%22,namespace=%22default%22%7D";
    static String topic5p2lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup5%22,topic=%22testtopic5%22,partition=%222%22,namespace=%22default%22%7D";
    static String topic5p3lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup5%22,topic=%22testtopic5%22,partition=%223%22,namespace=%22default%22%7D";
    static String topic5p4lag = "http://prometheus-operated:9090/api/v1/query?query=" +
            "kafka_consumergroup_lag%7Bconsumergroup=%22testgroup5%22,topic=%22testtopic5%22,partition=%224%22,namespace=%22default%22%7D";




    //// avg over time lag for topic 5


    static String topic5p0lagavg = "http://prometheus-operated:9090/api/v1/query?query=" +
            "avg_over_time(kafka_consumergroup_lag%7Bconsumergroup=~%22testgroup5%22,partition=%220%22,topic=%22testtopic5%22,namespace=%22default%22%7D%5B1m%5D)";
    static String topic5p1lagavg = "http://prometheus-operated:9090/api/v1/query?query=" +
            "avg_over_time(kafka_consumergroup_lag%7Bconsumergroup=~%22testgroup5%22,partition=%221%22,topic=%22testtopic5%22,namespace=%22default%22%7D%5B1m%5D)";
    static String topic5p2lagavg = "http://prometheus-operated:9090/api/v1/query?query=" +
            "avg_over_time(kafka_consumergroup_lag%7Bconsumergroup=~%22testgroup5%22,partition=%222%22,topic=%22testtopic5%22,namespace=%22default%22%7D%5B1m%5D)";
    static String topic5p3lagavg = "http://prometheus-operated:9090/api/v1/query?query=" +
            "avg_over_time(kafka_consumergroup_lag%7Bconsumergroup=~%22testgroup5%22,partition=%223%22,topic=%22testtopic5%22,namespace=%22default%22%7D%5B1m%5D)";
    static String topic5p4lagavg = "http://prometheus-operated:9090/api/v1/query?query=" +
            "avg_over_time(kafka_consumergroup_lag%7Bconsumergroup=~%22testgroup5%22,partition=%223%22,topic=%22testtopic5%22,namespace=%22default%22%7D%5B1m%5D)";

    static String topic5lagavg = "http://prometheus-operated:9090/api/v1/query?query=" +
            "sum(avg_over_time(kafka_consumergroup_lag%7Bconsumergroup=~%22testgroup5%22,topic=%22testtopic5%22,namespace=%22default%22%7D%5B1m%5D))%20by%20(topic)";

}


/////////////////////////////////////////////////////////////////////////////////////////
/*avg_over_time(kafka_consumergroup_lag{consumergroup=~"testgroup1",topic=~"testtopic1", partition= "3", namespace=~"default"}[1m])

        sum(kafka_consumergroup_lag{consumergroup=~"testgroup1",topic=~"testtopic1", namespace=~"default"}) by(topic)



        avg_over_time(kafka_consumergroup_lag{consumergroup=~"testgroup1",topic=~"testtopic1", namespace=~"default"}[1m])



        sum(avg_over_time(kafka_consumergroup_lag{consumergroup=~"testgroup1",topic=~"testtopic1", namespace=~"default"}[1m])) by (topic)*/


///////////////////////////////////////////////////////////////////////////////////////////