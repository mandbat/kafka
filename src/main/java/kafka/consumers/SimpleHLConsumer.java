package kafka.consumers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class SimpleHLConsumer {

	private ConsumerConnector consumer;
	private String topic;

	public SimpleHLConsumer(String zookeeper, String groupId, String topic) {
		ConsumerConfig consumerConfig = createConsumerConfig(zookeeper, groupId);
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}

	public static void main(String[] args) {
		String zooKeeper = "192.168.93.139:2181";
		String groupId = "group01";
		String topic = "test-rep";
		SimpleHLConsumer simpleHLConsumer = new SimpleHLConsumer(zooKeeper, groupId, topic);
		simpleHLConsumer.testConsumer();
	}

	public void testConsumer() {

		Map<String, Integer> topicMap = new HashMap<String, Integer>();

		// Define single thread for topic
		topicMap.put(topic, new Integer(1));

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);

		List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topic);

		for (final KafkaStream<byte[], byte[]> stream : streamList) {
			ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
			System.out.println("1******************");
			while (consumerIte.hasNext())
				System.out.println("Message from Single Topic :: " + new String(consumerIte.next().message()));
		}
		if (consumer != null)
			System.out.println("3******************");
			consumer.shutdown();
	}

}