/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.platform.environment.connection.destination;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.platform.data.dto.ConnectionValidationResult;
import io.debezium.platform.domain.views.Connection;
import io.debezium.platform.environment.connection.ConnectionValidator;

/**
 * Implementation of {@link ConnectionValidator} for Apache RocketMQ.
 * <p>
 * This validator validates RocketMQ connection configurations including:
 * <ul>
 *   <li>NameServer address connectivity</li>
 *   <li>Topic existence and accessibility</li>
 * </ul>
 * </p>
 *
 * <p>
 * Required configuration parameters:
 * <ul>
 *   <li><code>namesrvAddr</code> - NameServer address (e.g., "localhost:9876")</li>
 *   <li><code>topic</code> - Topic name to validate</li>
 * </ul>
 * </p>
 *
 * <p>
 * Optional configuration parameters:
 * <ul>
 *   <li><code>namespace</code> - Instance namespace</li>
 *   <li><code>producerGroup</code> - Producer group name (default: "DEFAULT_PRODUCER_VALIDATION_GROUP")</li>
 * </ul>
 * </p>
 *
 * <p>
 * Note: ACL authentication is not supported in the current implementation.
 * RocketMQ 5.x requires ACL to be configured through SessionCredentials or broker configuration.
 * </p>
 *
 * @author Pranav Kumar Tiwari
 */
@ApplicationScoped
@Named("APACHE_ROCKETMQ")
public class RocketMqConnectionValidator implements ConnectionValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMqConnectionValidator.class);

    private static final String NAMESRV_ADDR_KEY = "namesrvAddr";
    private static final String TOPIC_KEY = "topic";
    private static final String NAMESPACE_KEY = "namespace"; // Optional
    private static final String PRODUCER_GROUP_KEY = "producerGroup"; // Optional
    private static final String DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER_VALIDATION_GROUP";

    private final int defaultTimeout;

    public RocketMqConnectionValidator(
                                       @ConfigProperty(name = "destinations.rocketmq.connection.timeout") int defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
    }

    @Override
    public ConnectionValidationResult validate(Connection connectionConfig) {
        if (connectionConfig == null) {
            return ConnectionValidationResult.failed("Connection configuration cannot be null");
        }

        try {
            LOGGER.debug("Starting RocketMQ connection validation for: {}", connectionConfig.getName());

            Map<String, Object> rocketmqConfig = connectionConfig.getConfig();

            ConnectionValidationResult configValidation = validateConfiguration(rocketmqConfig);
            if (!configValidation.valid()) {
                return configValidation;
            }

            return performConnectionValidation(rocketmqConfig);
        }
        catch (Exception e) {
            LOGGER.error("Unexpected error during RocketMQ connection validation", e);
            return ConnectionValidationResult.failed("Unexpected error: " + e.getMessage());
        }
    }

    private ConnectionValidationResult validateConfiguration(Map<String, Object> config) {
        // Validate NameServer address
        if (!config.containsKey(NAMESRV_ADDR_KEY) || config.get(NAMESRV_ADDR_KEY) == null ||
                config.get(NAMESRV_ADDR_KEY).toString().trim().isEmpty()) {
            return ConnectionValidationResult.failed("NameServer address must be specified");
        }

        // Validate topic name
        if (!config.containsKey(TOPIC_KEY) || config.get(TOPIC_KEY) == null ||
                config.get(TOPIC_KEY).toString().trim().isEmpty()) {
            return ConnectionValidationResult.failed("Topic name must be specified");
        }

        return ConnectionValidationResult.successful();
    }

    private ConnectionValidationResult performConnectionValidation(Map<String, Object> config) {
        DefaultMQProducer producer = null;

        try {
            String namesrvAddr = config.get(NAMESRV_ADDR_KEY).toString().trim();
            String topic = config.get(TOPIC_KEY).toString().trim();
            String producerGroup = config.containsKey(PRODUCER_GROUP_KEY)
                    ? config.get(PRODUCER_GROUP_KEY).toString().trim()
                    : DEFAULT_PRODUCER_GROUP;

            LOGGER.debug("Connecting to RocketMQ NameServer: {}, Topic: {}", namesrvAddr, topic);

            // Create producer with configuration
            producer = new DefaultMQProducer(producerGroup);
            producer.setNamesrvAddr(namesrvAddr);

            // Set optional namespace if provided
            if (config.containsKey(NAMESPACE_KEY) && config.get(NAMESPACE_KEY) != null) {
                String namespace = config.get(NAMESPACE_KEY).toString().trim();
                if (!namespace.isEmpty()) {
                    producer.setNamespace(namespace);
                    LOGGER.debug("Using namespace: {}", namespace);
                }
            }

            // Note: ACL credentials (accessKey/secretKey) are not supported in this implementation
            // RocketMQ 5.x requires ACL to be configured through SessionCredentials or broker config
            // For production use with ACL, additional configuration is needed

            // Set send message timeout to configured value
            producer.setSendMsgTimeout(defaultTimeout * 1000);

            // Start the producer (required before any operations)
            producer.start();
            LOGGER.debug("RocketMQ producer started successfully");

            // Validate topic by fetching publish message queues
            // This will throw exception if topic doesn't exist or is not accessible
            var messageQueues = producer.fetchPublishMessageQueues(topic);

            if (messageQueues == null || messageQueues.isEmpty()) {
                String message = "Topic '" + topic + "' has no available message queues";
                LOGGER.warn(message);
                return ConnectionValidationResult.failed(message);
            }

            LOGGER.debug("Successfully validated RocketMQ topic '{}'. Available queues: {}",
                    topic, messageQueues.size());

            return ConnectionValidationResult.successful();

        }
        catch (MQClientException e) {
            String message = "RocketMQ client error: " + e.getErrorMessage();
            LOGGER.error(message, e);

            // Check for common error scenarios
            if (e.getErrorMessage() != null) {
                if (e.getErrorMessage().contains("connect to")) {
                    return ConnectionValidationResult.failed("Failed to connect to NameServer: Please verify the address.");
                }
                if (e.getErrorMessage().contains("No route info")) {
                    return ConnectionValidationResult.failed("Topic not found: Please verify the topic name exists in RocketMQ.");
                }
            }

            return ConnectionValidationResult.failed(message);
        }
        catch (Exception e) {
            LOGGER.warn("Generic exception during RocketMQ validation", e);
            return ConnectionValidationResult.failed("Failed to validate RocketMQ connection: " + e.getMessage());
        }
        finally {
            // Shutdown producer to release resources
            if (producer != null) {
                try {
                    producer.shutdown();
                    LOGGER.debug("RocketMQ producer shutdown completed");
                }
                catch (Exception ex) {
                    LOGGER.warn("Error shutting down RocketMQ producer", ex);
                }
            }
        }
    }
}
