/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.platform.environment.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.platform.data.dto.ConnectionValidationResult;
import io.debezium.platform.data.model.ConnectionEntity;
import io.debezium.platform.domain.views.Connection;
import io.debezium.platform.environment.connection.destination.RocketMqConnectionValidator;

/**
 * Unit tests for {@link RocketMqConnectionValidator}.
 * Tests configuration validation logic without actual RocketMQ connectivity.
 *
 * @author Pranav Kumar Tiwari
 */
class RocketMqConnectionValidatorTest {

    private static final int DEFAULT_TIMEOUT = 30;
    private RocketMqConnectionValidator validator;

    @BeforeEach
    void setUp() {
        validator = new RocketMqConnectionValidator(DEFAULT_TIMEOUT);
    }

    // ==================== NULL AND EMPTY CONFIGURATION TESTS ====================

    @Test
    @DisplayName("Should fail validation when connection config is null")
    void shouldFailValidationWhenConnectionConfigIsNull() {
        ConnectionValidationResult result = validator.validate(null);

        assertFalse(result.valid(), "Validation should fail for null connection config");
        assertEquals("Connection configuration cannot be null", result.message());
    }

    // ==================== NAMESRV ADDRESS VALIDATION TESTS ====================

    @Test
    @DisplayName("Should fail validation when NameServer address is missing")
    void shouldFailValidationWhenNamesrvAddrIsMissing() {
        Map<String, Object> config = new HashMap<>();
        config.put("topic", "test-topic");

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        assertFalse(result.valid(), "Validation should fail when NameServer address is missing");
        assertEquals("NameServer address must be specified", result.message());
    }

    @Test
    @DisplayName("Should fail validation when NameServer address is null")
    void shouldFailValidationWhenNamesrvAddrIsNull() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", null);
        config.put("topic", "test-topic");

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        assertFalse(result.valid(), "Validation should fail when NameServer address is null");
        assertEquals("NameServer address must be specified", result.message());
    }

    @Test
    @DisplayName("Should fail validation when NameServer address is empty string")
    void shouldFailValidationWhenNamesrvAddrIsEmpty() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", "");
        config.put("topic", "test-topic");

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        assertFalse(result.valid(), "Validation should fail when NameServer address is empty");
        assertEquals("NameServer address must be specified", result.message());
    }

    @Test
    @DisplayName("Should fail validation when NameServer address is only whitespace")
    void shouldFailValidationWhenNamesrvAddrIsWhitespace() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", "   ");
        config.put("topic", "test-topic");

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        assertFalse(result.valid(), "Validation should fail when NameServer address is only whitespace");
        assertEquals("NameServer address must be specified", result.message());
    }

    // ==================== TOPIC NAME VALIDATION TESTS ====================

    @Test
    @DisplayName("Should fail validation when topic name is missing")
    void shouldFailValidationWhenTopicNameIsMissing() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", "localhost:9876");

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        assertFalse(result.valid(), "Validation should fail when topic name is missing");
        assertEquals("Topic name must be specified", result.message());
    }

    @Test
    @DisplayName("Should fail validation when topic name is null")
    void shouldFailValidationWhenTopicNameIsNull() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", "localhost:9876");
        config.put("topic", null);

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        assertFalse(result.valid(), "Validation should fail when topic name is null");
        assertEquals("Topic name must be specified", result.message());
    }

    @Test
    @DisplayName("Should fail validation when topic name is empty string")
    void shouldFailValidationWhenTopicNameIsEmpty() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", "localhost:9876");
        config.put("topic", "");

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        assertFalse(result.valid(), "Validation should fail when topic name is empty");
        assertEquals("Topic name must be specified", result.message());
    }

    @Test
    @DisplayName("Should fail validation when topic name is only whitespace")
    void shouldFailValidationWhenTopicNameIsWhitespace() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", "localhost:9876");
        config.put("topic", "   ");

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        assertFalse(result.valid(), "Validation should fail when topic name is only whitespace");
        assertEquals("Topic name must be specified", result.message());
    }

    // ==================== BOTH MISSING TESTS ====================

    @Test
    @DisplayName("Should fail validation when both NameServer address and topic are missing")
    void shouldFailValidationWhenBothNamesrvAddrAndTopicAreMissing() {
        Map<String, Object> config = new HashMap<>();

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        assertFalse(result.valid(), "Validation should fail when both NameServer address and topic are missing");
        // Should fail on first check (NameServer address)
        assertEquals("NameServer address must be specified", result.message());
    }

    // ==================== OPTIONAL FIELDS TESTS ====================

    @Test
    @DisplayName("Should handle missing optional namespace field")
    void shouldHandleMissingNamespace() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", "localhost:9876");
        config.put("topic", "test-topic");
        // namespace is optional, not included

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        // Will fail trying to connect to non-existent server, but proves namespace is optional
        assertFalse(result.valid());
        assertTrue(result.message().contains("Failed to") || result.message().contains("error"),
                "Should fail on connectivity, not missing namespace");
    }

    @Test
    @DisplayName("Should handle missing optional producerGroup field")
    void shouldHandleMissingProducerGroup() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", "localhost:9876");
        config.put("topic", "test-topic");
        // producerGroup is optional, not included

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        // Will fail trying to connect, but proves producerGroup is optional
        assertFalse(result.valid());
        assertTrue(result.message().contains("Failed to") || result.message().contains("error"),
                "Should fail on connectivity, not missing producerGroup");
    }

    // ==================== EDGE CASES ====================

    @Test
    @DisplayName("Should handle very long topic name")
    void shouldHandleVeryLongTopicName() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", "localhost:9876");
        config.put("topic", "a".repeat(1000)); // Very long string

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        assertFalse(result.valid(), "Should handle very long topic name gracefully");
        assertTrue(result.message().contains("Failed to") || result.message().contains("error"),
                "Should fail with appropriate error");
    }

    @Test
    @DisplayName("Should handle special characters in topic name")
    void shouldHandleSpecialCharactersInTopicName() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", "localhost:9876");
        config.put("topic", "test-topic_123.ABC-%abc");

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        assertFalse(result.valid(), "Should process special characters in topic name");
        assertTrue(result.message().contains("Failed to") || result.message().contains("error"),
                "Should attempt validation with special characters");
    }

    @Test
    @DisplayName("Should handle invalid NameServer address format")
    void shouldHandleInvalidNamesrvAddrFormat() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", "not-a-valid-address");
        config.put("topic", "test-topic");

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        assertFalse(result.valid(), "Should handle invalid NameServer address format");
        assertTrue(result.message().contains("Failed to") || result.message().contains("error"),
                "Should fail with appropriate error for invalid address");
    }

    @Test
    @DisplayName("Should handle NameServer address with port")
    void shouldHandleNamesrvAddrWithPort() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", "localhost:9876");
        config.put("topic", "test-topic");

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        // Will fail connecting to non-existent server, but validates format is accepted
        assertFalse(result.valid());
        assertTrue(result.message().contains("Failed to") || result.message().contains("error"),
                "Should process address with port");
    }

    @Test
    @DisplayName("Should handle multiple NameServer addresses")
    void shouldHandleMultipleNamesrvAddresses() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", "localhost:9876;localhost:9877");
        config.put("topic", "test-topic");

        Connection connection = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(connection);

        // Will fail connecting, but validates format is accepted
        assertFalse(result.valid());
        assertTrue(result.message().contains("Failed to") || result.message().contains("error"),
                "Should process multiple NameServer addresses");
    }
}
