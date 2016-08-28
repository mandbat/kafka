package kafka.consumers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import util.MyConsoleWriter;

public class SimpleHLConsumer implements Runnable {

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
		String zookeeper = "192.168.93.139:2181";
		String groupId = "group";
		String topic = "test2";
		// SimpleHLConsumer simpleHLConsumer = new SimpleHLConsumer(zookeeper,
		// groupId, topic);
		// simpleHLConsumer.testConsumer();
		Thread t1 = new Thread(new SimpleHLConsumer(zookeeper, groupId + "_1", topic));
		Thread t2 = new Thread(new SimpleHLConsumer(zookeeper, groupId + "_2", topic));
		Thread t3 = new Thread(new SimpleHLConsumer(zookeeper, groupId + "_3", topic));
		
		t1.setName("T1");
		t2.setName("T2");
		t3.setName("T3");
		
		t1.start();
		t2.start();
		t3.start();
	}

	// public void testConsumer() {
	//
	// Map<String, Integer> topicMap = new HashMap<String, Integer>();
	//
	// // Define single thread for topic
	// topicMap.put(topic, new Integer(1));
	//
	// Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap =
	// consumer.createMessageStreams(topicMap);
	//
	// List<KafkaStream<byte[], byte[]>> streamList =
	// consumerStreamsMap.get(topic);
	//
	// for (final KafkaStream<byte[], byte[]> stream : streamList) {
	// ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
	// while (consumerIte.hasNext())
	// System.out.println("Message from Single Topic :: " + new
	// String(consumerIte.next().message()));
	// }
	// if (consumer != null)
	// consumer.shutdown();
	// }

	@Override
	public void run() {

		Map<String, Integer> topicMap = new HashMap<String, Integer>();

		// Define single thread for topic
		topicMap.put(topic, new Integer(1));

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);

		List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topic);

		for (final KafkaStream<byte[], byte[]> stream : streamList) {
			ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
			while (consumerIte.hasNext()) {
				// System.out.println("Message from Single Topic:" +
				// Thread.currentThread().getName() + ":"
				// + new String(consumerIte.next().message()));
				prn(Thread.currentThread(), consumerIte);
			}

		}
		if (consumer != null)
			consumer.shutdown();

	}

	public void prn(Thread t, ConsumerIterator<byte[], byte[]> consumerIte) {
		String txt = "Message from Single Topic : " + Thread.currentThread().getName() + " : "
				+ new String(consumerIte.next().message());
		System.out.println(txt);
		MyConsoleWriter.write(txt, "d:\\111.log");
	}

}