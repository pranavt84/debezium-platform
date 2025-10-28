/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.platform.environment.database.db;

import java.time.Duration;
import java.util.Map;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/**
 * Testcontainers resource for Apache RocketMQ with NameServer and Broker.
 * <p>
 * This resource manages the lifecycle of RocketMQ containers for integration testing.
 * It provides a local RocketMQ instance without requiring external infrastructure.
 * </p>
 *
 * <p>
 * The resource performs the following setup:
 * <ul>
 *   <li>Creates a Docker network for container communication</li>
 *   <li>Starts a RocketMQ NameServer container</li>
 *   <li>Starts a RocketMQ Broker container</li>
 *   <li>Creates a test topic named "test-topic"</li>
 *   <li>Waits for services to become ready</li>
 *   <li>Provides configuration properties for connecting to the local instance</li>
 * </ul>
 * </p>
 *
 * <p>
 * Configuration properties exposed to tests:
 * <ul>
 *   <li><code>rocketmq.namesrvAddr</code> - NameServer address</li>
 *   <li><code>rocketmq.topic</code> - The pre-created test topic name</li>
 * </ul>
 * </p>
 *
 * <p>
 * Usage in integration tests:
 * <pre>
 * {@code
 * @QuarkusTest
 * @QuarkusTestResource(RocketMqTestResource.class)
 * public class RocketMqConnectionValidatorIT {
 *     @Test
 *     void testRocketMqConnection() {
 *         String namesrvAddr = ConfigProvider.getConfig().getValue("rocketmq.namesrvAddr", String.class);
 *         String topic = ConfigProvider.getConfig().getValue("rocketmq.topic", String.class);
 *         // Use these properties to test RocketMQ connection
 *     }
 * }
 * }
 * </pre>
 * </p>
 *
 * @author Pranav Kumar Tiwari
 */
public class RocketMqTestResource implements QuarkusTestResourceLifecycleManager {

    private static final String ROCKETMQ_IMAGE = "apache/rocketmq:4.9.8";
    private static final String NAMESRV_ALIAS = "rmqnamesrv";
    private static final String BROKER_ALIAS = "rmqbroker";
    private static final int NAMESRV_PORT = 9876;
    private static final int BROKER_PORT = 10909;
    private static final int BROKER_VIP_PORT = 10911;
    private static final String TEST_TOPIC = "test-topic";

    private Network network;
    private GenericContainer<?> namesrvContainer;
    private GenericContainer<?> brokerContainer;

    /**
     * Starts the RocketMQ NameServer and Broker containers.
     * This method is called before any tests run.
     *
     * @return Map of configuration properties to inject into test context
     */
    @Override
    public Map<String, String> start() {
        try {
            // Create a network for containers to communicate
            network = Network.newNetwork();

            // Start NameServer container
            namesrvContainer = new GenericContainer<>(DockerImageName.parse(ROCKETMQ_IMAGE))
                    .withNetwork(network)
                    .withNetworkAliases(NAMESRV_ALIAS)
                    .withExposedPorts(NAMESRV_PORT)
                    .withCommand("sh", "mqnamesrv")
                    .waitingFor(Wait.forLogMessage(".*The Name Server boot success.*", 1)
                            .withStartupTimeout(Duration.ofSeconds(60)));

            namesrvContainer.start();

            // Get the NameServer address that will be used by broker and tests
            String namesrvAddr = namesrvContainer.getHost() + ":" + namesrvContainer.getMappedPort(NAMESRV_PORT);

            // Start Broker container
            brokerContainer = new GenericContainer<>(DockerImageName.parse(ROCKETMQ_IMAGE))
                    .withNetwork(network)
                    .withNetworkAliases(BROKER_ALIAS)
                    .withExposedPorts(BROKER_PORT, BROKER_VIP_PORT)
                    .withEnv("NAMESRV_ADDR", NAMESRV_ALIAS + ":" + NAMESRV_PORT)
                    .withCommand("sh", "mqbroker", "-n", NAMESRV_ALIAS + ":" + NAMESRV_PORT,
                            "-c", "/home/rocketmq/rocketmq-4.9.8/conf/broker.conf")
                    .dependsOn(namesrvContainer)
                    .waitingFor(Wait.forLogMessage(".*The broker.*boot success.*", 1)
                            .withStartupTimeout(Duration.ofSeconds(90)));

            brokerContainer.start();

            // Wait a bit for broker to fully initialize
            Thread.sleep(5000);

            // Create test topic using admin operations
            createTestTopic(namesrvAddr);

            // Return configuration properties for tests to use
            return Map.of(
                    "rocketmq.namesrvAddr", namesrvAddr,
                    "rocketmq.topic", TEST_TOPIC);

        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start RocketMQ test containers", e);
        }
    }

    /**
     * Stops the RocketMQ containers and cleans up network.
     * This method is called after all tests have completed.
     */
    @Override
    public void stop() {
        if (brokerContainer != null) {
            try {
                brokerContainer.stop();
            }
            catch (Exception e) {
                // Log but don't fail
                System.err.println("Error stopping broker container: " + e.getMessage());
            }
        }

        if (namesrvContainer != null) {
            try {
                namesrvContainer.stop();
            }
            catch (Exception e) {
                // Log but don't fail
                System.err.println("Error stopping namesrv container: " + e.getMessage());
            }
        }

        if (network != null) {
            try {
                network.close();
            }
            catch (Exception e) {
                // Log but don't fail
                System.err.println("Error closing network: " + e.getMessage());
            }
        }
    }

    /**
     * Creates a test topic in RocketMQ by sending a test message.
     * RocketMQ will auto-create the topic when the first message is sent.
     *
     * @param namesrvAddr the NameServer address to connect to
     * @throws Exception if topic creation fails
     */
    private void createTestTopic(String namesrvAddr) throws Exception {
        DefaultMQProducer producer = null;
        try {
            // Create producer to send test message (this will create the topic)
            producer = new DefaultMQProducer("test-producer-group");
            producer.setNamesrvAddr(namesrvAddr);
            producer.setSendMsgTimeout(10000);
            producer.setRetryTimesWhenSendFailed(3);
            producer.start();

            // Send a test message to create the topic
            Message msg = new Message(TEST_TOPIC, "test-tag", "Test message to create topic".getBytes());
            producer.send(msg);

            // Wait for topic to be fully created
            Thread.sleep(2000);

            System.out.println("Successfully created test topic: " + TEST_TOPIC);

        }
        finally {
            if (producer != null) {
                producer.shutdown();
            }
        }
    }
}
