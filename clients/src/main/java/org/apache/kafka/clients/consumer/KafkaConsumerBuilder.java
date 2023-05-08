package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.ClientDnsLookup;

import java.util.List;
import java.util.Properties;

public class KafkaConsumerBuilder<K,V> {

    private Properties properties = new Properties();

    public KafkaConsumerBuilder(){

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

    /**
     * The timeout used to detect client failures when using
     * Kafka's group management facility. The client sends periodic heartbeats to indicate its liveness
     * to the broker. If no heartbeats are received by the broker before the expiration of this session timeout,
     * then the broker will remove this client from the group and initiate a rebalance. Note that the value
     * must be in the allowable range as configured in the broker configuration by <code>group.min.session.timeout.ms</code>
     * and <code>group.max.session.timeout.ms</code>.
     * @param sessionTimeoutMs
     * @return updated builder
     */
    public KafkaConsumerBuilder withSessionTimeoutMsConfig(int sessionTimeoutMs){
        this.properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,sessionTimeoutMs);
        return this;
    }

    /**
     * The expected time between heartbeats to the consumer
     * coordinator when using Kafka's group management facilities. Heartbeats are used to ensure that the
     * consumer's session stays active and to facilitate rebalancing when new consumers join or leave the group.
     * The value must be set lower than <code>session.timeout.ms</code>, but typically should be set no higher
     * than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.
     * @param heartbeatIntervalMs
     * @return
     */
    public KafkaConsumerBuilder withHeartbeatIntervalMs(int heartbeatIntervalMs){
        this.properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        return this;
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
     *
     * @param clientDnsLookup
     * @return
     */
    public KafkaConsumerBuilder withClientDnsLookup(ClientDnsLookup clientDnsLookup){
        this.properties.put(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG, clientDnsLookup.toString());
        return this;
    }




    public KafkaConsumer build(){
        Properties properties = new Properties();
        KafkaConsumer<K,V> consumer = new KafkaConsumer<K, V>(properties);

        return consumer;
    }
}
