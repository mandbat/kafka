package kafka.producers;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleProducer {

	private static Producer<String, String> producer;

	public SimpleProducer() {
		Properties props = new Properties();

		// Set the broker list for requesting metadata to find the lead broker
		props.put("metadata.broker.list", "192.168.93.139:9091, 192.168.93.139:9092");

		// This specifies the serializer class for keys
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("producer.type", "sync");

		// 1 means the producer receives an acknowledgment once the lead replica
		// has received the data. This option provides better durability as the
		// client waits until the server acknowledges the request as successful.
		props.put("request.required.acks", "1");

		// props.put("compression.codec", "gzip");
		// props.put("batch.num.messages", "5");

		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}

	public static void main(String[] args) throws InterruptedException {
		// int argsCount = args.length;
		// if (argsCount == 0 || argsCount == 1)
		// throw new IllegalArgumentException("Please provide topic name and
		// Message count as arguments");

		// Topic name and the message count to be published is passed from the
		// command line
		// String topic = (String) args[0];
		// String count = (String) args[1];

		String topic = "test-rep";
		String count = "50";

		int messageCount = Integer.parseInt(count);
		System.out.println("Topic Name - " + topic);
		System.out.println("Message Count - " + messageCount);

		SimpleProducer simpleProducer = new SimpleProducer();
		simpleProducer.publishMessage(topic, messageCount);
	}

	private void publishMessage(String topic, int messageCount) throws InterruptedException {
		for (int mCount = 0; mCount < messageCount; mCount++) {

			String runtime = new Date().toString();

			String msg = "Message Publishing Time - " + String.valueOf(mCount) + ", " + runtime;
			// System.out.println(msg);
			// Creates a KeyedMessage instance
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);

			// Publish the message
			producer.send(data);
			Thread.sleep(3000);
		}
		// Close producer connection with broker.
		producer.close();
	}
}
