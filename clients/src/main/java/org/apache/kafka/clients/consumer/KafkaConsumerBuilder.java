package org.apache.kafka.clients.consumer;

import java.util.List;
import java.util.Properties;

public class KafkaConsumerBuilder<K,V> {

    private Properties properties = new Properties();

    public KafkaConsumerBuilder(){

    }

    /**
     * A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
     * The client will make use of all servers irrespective of which servers are specified here for bootstrapping&mdash;this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form
     * <code>host1:port1,host2:port2,...</code>. Since these servers are just used for the initial connection to
     * discover the full cluster membership (which may change dynamically), this list need not contain the full set of
     * servers (you may want more than one, though, in case a server is down).
     * @param bootstrapServers
     * @return updated builder
     */
    public KafkaConsumerBuilder withBootstrapServers(List<String> bootstrapServers){
        this.properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return this;
    }

    /**
     * A unique string that identifies the consumer group this consumer belongs to.
     * This property is required if the consumer uses either the group management functionality by using
     * <code>subscribe(topic)</code> or the Kafka-based offset management strategy.
     * @param groupIdConfig
     * @return updated builder
     */
    public KafkaConsumerBuilder withGroupIdConfig(String groupIdConfig){
        this.properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdConfig);
        return this;
    }

    /**
     * The maximum number of records returned in a single call to poll().
     * Note, that <code>" + MAX_POLL_RECORDS_CONFIG + "</code> does not impact the underlying fetching behavior.
     * The consumer will cache the records from each fetch request and returns them incrementally from each poll.
     * @param maxPollRecords
     * @return updated builder
     */
    public KafkaConsumerBuilder withMaxPollRecords(int maxPollRecords){
        this.properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        return this;
    }

    /**
     * The maximum delay between invocations of poll() when using "
     * consumer group management. This places an upper bound on the amount of time that the consumer can be idle
     * before fetching more records. If poll() is not called before expiration of this timeout, then the consumer
     * is considered failed and the group will rebalance in order to reassign the partitions to another member.
     * For consumers using a non-null <code>group.instance.id</code> which reach this timeout, partitions will not be immediately reassigned.
     * Instead, the consumer will stop sending heartbeats and partitions will be reassigned
     * after expiration of <code>session.timeout.ms</code>. This mirrors the behavior of a static consumer which has shutdown.
     * @param maxPollIntervalMs
     * @return updated builder
     */

    public KafkaConsumerBuilder withMaxPollIntervalMs(int maxPollIntervalMs){
        this.properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        return this;
    }



    public KafkaConsumer build(){
        Properties properties = new Properties();
        KafkaConsumer<K,V> consumer = new KafkaConsumer<K, V>(properties);

        return consumer;
    }
}
