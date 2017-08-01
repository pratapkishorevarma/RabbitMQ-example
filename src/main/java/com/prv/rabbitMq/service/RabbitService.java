package com.prv.rabbitMq.service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.Exchange.DeclareOk;
import com.rabbitmq.client.AMQP.Exchange.DeleteOk;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitService {

	private String serverUri;

	public RabbitService(String serverUri) {
		super();
		this.serverUri = serverUri;
	}

	public void createExchangeAndQueueAndBindQueue(String exchangeName, String queueName, String bindingKey) {
		Connection connection = null;
		Channel channel = null;

		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri(this.serverUri);

			// The connection abstracts the socket connection, and takes care of
			// protocol version negotiation and authentication etc..
			connection = factory.newConnection();
			channel = connection.createChannel();

			// Create topic exchange
			DeclareOk exchangeDeclare = channel.exchangeDeclare(exchangeName, BuiltinExchangeType.TOPIC);
			com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclare = channel.queueDeclare(queueName, false, false, false,
					null);

			channel.queueBind(queueName, exchangeName, bindingKey);

			System.out.println("Created exchange " + exchangeName + ":" + exchangeDeclare);
			System.out.println("Created queue " + queueName + ":" + queueDeclare + " and bound to :" + bindingKey);
		} catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException | IOException
				| TimeoutException e) {
			System.out.println("Exception: " + e.getMessage());
		} finally {
			closeResources(connection, channel);
		}
	}

	public void deleteExchange(String exchangeName) {
		Connection connection = null;
		Channel channel = null;

		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri(this.serverUri);

			// The connection abstracts the socket connection, and takes care of
			// protocol version negotiation and authentication etc..
			connection = factory.newConnection();
			channel = connection.createChannel();

			DeleteOk exchangeDelete = channel.exchangeDelete(exchangeName);

			System.out.println("Deleted exchange " + exchangeName + ":" + exchangeDelete);
		} catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException | IOException
				| TimeoutException e) {
			System.out.println("Exception: " + e.getMessage());
		} finally {
			closeResources(connection, channel);
		}
	}

	public void createQueue(String queueName) {
		Connection connection = null;
		Channel channel = null;

		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri(this.serverUri);

			connection = factory.newConnection();
			channel = connection.createChannel();

			com.rabbitmq.client.AMQP.Queue.DeclareOk queueDeclare = channel.queueDeclare(queueName, false, false, false,
					null);

			System.out.println("Created queue " + queueName + ":" + queueDeclare);
		} catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException | IOException
				| TimeoutException e) {
			System.out.println("Exception: " + e.getMessage());
		} finally {
			closeResources(connection, channel);
		}
	}

	public void deleteQueue(String queueName) {
		Connection connection = null;
		Channel channel = null;

		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri(this.serverUri);

			connection = factory.newConnection();
			channel = connection.createChannel();

			com.rabbitmq.client.AMQP.Queue.DeleteOk queueDelete = channel.queueDelete(queueName, false, false);

			System.out.println("Deleted queue " + queueName + ":" + queueDelete);
		} catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException | IOException
				| TimeoutException e) {
			System.out.println("Exception: " + e.getMessage());
		} finally {
			closeResources(connection, channel);
		}
	}

	private void closeResources(Connection connection, Channel channel) {
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
