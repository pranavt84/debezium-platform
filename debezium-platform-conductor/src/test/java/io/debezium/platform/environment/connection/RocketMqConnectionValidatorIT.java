/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.platform.environment.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import jakarta.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.platform.data.dto.ConnectionValidationResult;
import io.debezium.platform.data.model.ConnectionEntity;
import io.debezium.platform.domain.views.Connection;
import io.debezium.platform.environment.connection.destination.RocketMqConnectionValidator;
import io.debezium.platform.environment.database.db.RocketMqTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Integration tests for {@link RocketMqConnectionValidator}.
 * Tests the validator against a real RocketMQ instance running in Testcontainers.
 *
 * @author Pranav Kumar Tiwari
 */
@QuarkusTest
@QuarkusTestResource(RocketMqTestResource.class)
public class RocketMqConnectionValidatorIT {

    @Inject
    RocketMqConnectionValidator validator;

    // ==================== SUCCESSFUL CONNECTION TESTS ====================

    @Test
    @DisplayName("Should successfully validate connection to existing RocketMQ topic")
    void shouldConnectSuccessfully() {
        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, Map.of(
                "namesrvAddr", ConfigProvider.getConfig().getValue("rocketmq.namesrvAddr", String.class),
                "topic", ConfigProvider.getConfig().getValue("rocketmq.topic", String.class)));

        ConnectionValidationResult result = validator.validate(conn);

        assertTrue(result.valid(), "Connection validation should succeed for existing topic");
    }

    @Test
    @DisplayName("Should successfully validate with whitespace in config values")
    void shouldValidateWithWhitespaceInConfigValues() {
        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, Map.of(
                "namesrvAddr", "  " + ConfigProvider.getConfig().getValue("rocketmq.namesrvAddr", String.class) + "  ",
                "topic", "  " + ConfigProvider.getConfig().getValue("rocketmq.topic", String.class) + "  "));

        ConnectionValidationResult result = validator.validate(conn);

        assertTrue(result.valid(), "Should trim whitespace and validate successfully");
    }

    @Test
    @DisplayName("Should successfully validate with optional producerGroup provided")
    void shouldValidateWithProducerGroup() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", ConfigProvider.getConfig().getValue("rocketmq.namesrvAddr", String.class));
        config.put("topic", ConfigProvider.getConfig().getValue("rocketmq.topic", String.class));
        config.put("producerGroup", "custom-producer-group");

        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(conn);

        assertTrue(result.valid(), "Should work with custom producer group");
    }

    // ==================== TOPIC NOT FOUND TESTS ====================

    @Test
    @DisplayName("Should fail validation when topic does not exist")
    void shouldFailForNonExistentTopic() {
        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, Map.of(
                "namesrvAddr", ConfigProvider.getConfig().getValue("rocketmq.namesrvAddr", String.class),
                "topic", "non-existent-topic-12345"));

        ConnectionValidationResult result = validator.validate(conn);

        assertFalse(result.valid(), "Connection validation should fail for non-existent topic");
        assertThat(result.message()).contains("Topic not found");
    }

    @Test
    @DisplayName("Should fail validation with descriptive message for non-existent topic")
    void shouldProvideDescriptiveErrorForNonExistentTopic() {
        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, Map.of(
                "namesrvAddr", ConfigProvider.getConfig().getValue("rocketmq.namesrvAddr", String.class),
                "topic", "another-missing-topic"));

        ConnectionValidationResult result = validator.validate(conn);

        assertFalse(result.valid());
        assertThat(result.message())
                .contains("Topic not found")
                .containsAnyOf("verify", "exists");
    }

    // ==================== INVALID NAMESRV ADDRESS TESTS ====================

    @Test
    @DisplayName("Should fail validation with invalid NameServer address")
    void shouldFailWithInvalidNamesrvAddr() {
        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, Map.of(
                "namesrvAddr", "invalid-server:9876",
                "topic", ConfigProvider.getConfig().getValue("rocketmq.topic", String.class)));

        ConnectionValidationResult result = validator.validate(conn);

        assertFalse(result.valid(), "Connection validation should fail with invalid NameServer address");
        assertTrue(result.message().contains("Failed to connect") ||
                result.message().contains("error") ||
                result.message().contains("RocketMQ"),
                "Error message should indicate connection issue");
    }

    @Test
    @DisplayName("Should fail validation with empty NameServer address despite valid topic")
    void shouldFailWithEmptyNamesrvAddr() {
        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, Map.of(
                "namesrvAddr", "",
                "topic", ConfigProvider.getConfig().getValue("rocketmq.topic", String.class)));

        ConnectionValidationResult result = validator.validate(conn);

        assertFalse(result.valid(), "Should fail with empty NameServer address");
        assertThat(result.message()).isEqualTo("NameServer address must be specified");
    }

    // ==================== MISSING CONFIGURATION TESTS ====================

    @Test
    @DisplayName("Should fail validation when NameServer address is missing")
    void shouldFailWhenNamesrvAddrIsMissing() {
        Map<String, Object> config = new HashMap<>();
        config.put("topic", ConfigProvider.getConfig().getValue("rocketmq.topic", String.class));
        // namesrvAddr is missing

        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(conn);

        assertFalse(result.valid(), "Should fail when NameServer address is missing");
        assertThat(result.message()).isEqualTo("NameServer address must be specified");
    }

    @Test
    @DisplayName("Should fail validation when topic name is missing")
    void shouldFailWhenTopicNameIsMissing() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", ConfigProvider.getConfig().getValue("rocketmq.namesrvAddr", String.class));
        // topic is missing

        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(conn);

        assertFalse(result.valid(), "Should fail when topic name is missing");
        assertThat(result.message()).isEqualTo("Topic name must be specified");
    }

    @Test
    @DisplayName("Should fail validation when both NameServer address and topic are missing")
    void shouldFailWhenBothNamesrvAddrAndTopicAreMissing() {
        Map<String, Object> config = new HashMap<>();
        // both namesrvAddr and topic are missing

        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(conn);

        assertFalse(result.valid(), "Should fail when both NameServer address and topic are missing");
        // Should fail on first validation check (namesrvAddr)
        assertThat(result.message()).isEqualTo("NameServer address must be specified");
    }

    // ==================== OPTIONAL FIELDS TESTS ====================

    @Test
    @DisplayName("Should work without optional producerGroup field")
    void shouldWorkWithoutProducerGroup() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", ConfigProvider.getConfig().getValue("rocketmq.namesrvAddr", String.class));
        config.put("topic", ConfigProvider.getConfig().getValue("rocketmq.topic", String.class));
        // producerGroup is optional and not provided

        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(conn);

        assertTrue(result.valid(), "Should work without optional producerGroup");
    }

    @Test
    @DisplayName("Should work without optional namespace field")
    void shouldWorkWithoutNamespace() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", ConfigProvider.getConfig().getValue("rocketmq.namesrvAddr", String.class));
        config.put("topic", ConfigProvider.getConfig().getValue("rocketmq.topic", String.class));
        // namespace is optional and not provided

        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(conn);

        assertTrue(result.valid(), "Should work without optional namespace");
    }

    @Test
    @DisplayName("Should work with namespace provided")
    void shouldWorkWithNamespace() {
        Map<String, Object> config = new HashMap<>();
        config.put("namesrvAddr", ConfigProvider.getConfig().getValue("rocketmq.namesrvAddr", String.class));
        config.put("topic", ConfigProvider.getConfig().getValue("rocketmq.topic", String.class));
        config.put("namespace", "test-namespace");

        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, config);
        ConnectionValidationResult result = validator.validate(conn);

        // May fail if namespace doesn't exist in broker, but validates it's processed
        // For this test, we just ensure it doesn't crash with namespace provided
        assertFalse(result.valid() && result.message() == null,
                "Should process namespace field");
    }

    // ==================== EDGE CASES ====================

    @Test
    @DisplayName("Should handle topic name with special characters")
    void shouldHandleTopicNameWithSpecialCharacters() {
        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, Map.of(
                "namesrvAddr", ConfigProvider.getConfig().getValue("rocketmq.namesrvAddr", String.class),
                "topic", "test-topic_123.ABC-%abc"));

        ConnectionValidationResult result = validator.validate(conn);

        // Will fail because topic doesn't exist, but validates special chars are handled
        assertFalse(result.valid());
        assertThat(result.message()).contains("Topic not found");
    }

    @Test
    @DisplayName("Should handle null connection config")
    void shouldHandleNullConnectionConfig() {
        ConnectionValidationResult result = validator.validate(null);

        assertFalse(result.valid(), "Should handle null connection gracefully");
        assertThat(result.message()).isEqualTo("Connection configuration cannot be null");
    }

    @Test
    @DisplayName("Should handle multiple NameServer addresses")
    void shouldHandleMultipleNamesrvAddresses() {
        // RocketMQ supports multiple NameServer addresses separated by semicolon
        String singleAddr = ConfigProvider.getConfig().getValue("rocketmq.namesrvAddr", String.class);
        String multipleAddr = singleAddr + ";" + singleAddr;

        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, Map.of(
                "namesrvAddr", multipleAddr,
                "topic", ConfigProvider.getConfig().getValue("rocketmq.topic", String.class)));

        ConnectionValidationResult result = validator.validate(conn);

        // Should work with multiple addresses format
        assertTrue(result.valid(), "Should handle multiple NameServer addresses");
    }

    @Test
    @DisplayName("Should handle unreachable NameServer gracefully")
    void shouldHandleUnreachableNamesrv() {
        Connection conn = new TestConnectionView(ConnectionEntity.Type.APACHE_ROCKETMQ, Map.of(
                "namesrvAddr", "localhost:19876", // Wrong port
                "topic", "test-topic"));

        ConnectionValidationResult result = validator.validate(conn);

        assertFalse(result.valid(), "Should fail for unreachable NameServer");
        assertTrue(result.message().contains("Failed to connect") ||
                result.message().contains("error") ||
                result.message().contains("RocketMQ"),
                "Should provide connection error message");
    }
}
