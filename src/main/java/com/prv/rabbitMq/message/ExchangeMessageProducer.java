package com.prv.rabbitMq.message;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class ExchangeMessageProducer implements Runnable {

	private String serverUri;
	private String exchangeName;
	private String routingKey;

	public ExchangeMessageProducer(String serverUri, String exchangeName, String routingKey) {
		super();
		this.serverUri = serverUri;
		this.exchangeName = exchangeName;
		this.routingKey = routingKey;
	}

	public void run() {
		sendMessage();
	}

	private void sendMessage() {
		Connection connection = null;
		Channel channel = null;

		try {

			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri(this.serverUri);

			// The connection abstracts the socket connection, and takes care of
			// protocol version negotiation and authentication etc..
			connection = factory.newConnection();
			channel = connection.createChannel();

			System.out.println("Starting Message producer ...");
			// Declaring a queue is idempotent - it will only be created if it
			// doesn't exist already

			int i = 0;

			while (true) {
				String message = "Message " + i;
				channel.basicPublish(this.exchangeName, this.routingKey, null, message.getBytes());
				System.out.println("Sent '" + message + "'");
				i++;
				Thread.sleep(2000);
			}

		} catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException | IOException | TimeoutException
				| InterruptedException e) {
			System.out.println("Exception: " + e.getMessage());
		} finally {
			System.out.println("Closing channel, connection.");
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
