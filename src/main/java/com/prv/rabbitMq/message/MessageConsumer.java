package com.prv.rabbitMq.message;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class MessageConsumer implements Runnable {

	private String serverUri;
	private String queueName;

	public MessageConsumer(String serverUri, String queueName) {
		super();
		this.queueName = queueName;
		this.serverUri = serverUri;
	}

	public void run() {
		consumeMessage();
	}

	private void consumeMessage() {
		Connection connection = null;
		Channel channel = null;

		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri(this.serverUri);

			// The connection abstracts the socket connection, and takes care of
			// protocol version negotiation and authentication etc..
			connection = factory.newConnection();
			channel = connection.createChannel();

			System.out.println("Starting Message consumer ...");
			Consumer consumer = new DefaultConsumer(channel) {
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
						byte[] body) throws IOException {
					String message = new String(body, "UTF-8");
					System.out.println("Received '" + message + "'");
				}
			};

			while (true) {
				channel.basicConsume(this.queueName, true, consumer);
			}

		} catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException | IOException
				| TimeoutException e) {
			System.out.println("Exception: " + e.getMessage());
		} finally {
			System.out.println("Closing consumer channel, connection.");
			if (channel != null) {
				try {
					channel.close();
				} catch (IOException | TimeoutException e) {
				}
			}

			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {

				}
			}
		}

	}

}
