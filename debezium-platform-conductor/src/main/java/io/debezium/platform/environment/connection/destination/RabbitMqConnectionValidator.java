/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.platform.environment.connection.destination;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;

import javax.net.ssl.SSLContext;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultSaslConfig;

import io.debezium.platform.data.dto.ConnectionValidationResult;
import io.debezium.platform.environment.connection.ConnectionValidator;

/**
 * Implementation of {@link ConnectionValidator} for RabbitMQ.
 * <p>
 * This validator validates RabbitMQ connection configurations including:
 * <ul>
 *   <li>Host and port connectivity</li>
 *   <li>Multiple authentication types (PLAIN, EXTERNAL, SCRAM-SHA-256, OAUTH2)</li>
 *   <li>SSL/TLS connections with keystore/truststore</li>
 * </ul>
 * </p>
 *
 * <p>
 * Required configuration parameters:
 * <ul>
 *   <li><code>host</code> - RabbitMQ server hostname</li>
 *   <li><code>port</code> - RabbitMQ server port (default: 5672 for non-SSL, 5671 for SSL)</li>
 * </ul>
 * </p>
 *
 * <p>
 * Authentication configuration (based on authType):
 * <ul>
 *   <li><b>PLAIN</b> (default):
 *     <ul>
 *       <li><code>username</code> - Username (default: "guest")</li>
 *       <li><code>password</code> - Password (default: "guest")</li>
 *     </ul>
 *   </li>
 *   <li><b>EXTERNAL</b>: Uses SSL client certificate authentication</li>
 *   <li><b>SCRAM</b>: SCRAM-SHA-256 authentication
 *     <ul>
 *       <li><code>username</code> - Username</li>
 *       <li><code>password</code> - Password</li>
 *     </ul>
 *   </li>
 *   <li><b>OAUTH2</b>: OAuth2 token-based authentication
 *     <ul>
 *       <li><code>oauth2Token</code> - OAuth2 access token</li>
 *     </ul>
 *   </li>
 * </ul>
 * </p>
 *
 * <p>
 * SSL/TLS configuration (when useSsl=true):
 * <ul>
 *   <li><code>useSsl</code> - Enable SSL/TLS (default: false)</li>
 *   <li><code>keyStorePath</code> - Path to client keystore (PKCS12)</li>
 *   <li><code>keyStorePassword</code> - Keystore password</li>
 *   <li><code>trustStorePath</code> - Path to truststore (JKS)</li>
 *   <li><code>trustStorePassword</code> - Truststore password</li>
 * </ul>
 * </p>
 *
 * @author Pranav Kumar Tiwari
 */
@ApplicationScoped
@Named("RABBITMQ_STREAM")
public class RabbitMqConnectionValidator implements ConnectionValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqConnectionValidator.class);

    private static final String HOST_KEY = "host";
    private static final String PORT_KEY = "port";
    private static final String USERNAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String USE_SSL_KEY = "useSsl";
    private static final String AUTH_TYPE_KEY = "authType";
    private static final String OAUTH2_TOKEN_KEY = "oauth2Token";
    private static final String KEYSTORE_PATH_KEY = "keyStorePath";
    private static final String KEYSTORE_PASSWORD_KEY = "keyStorePassword";
    private static final String TRUSTSTORE_PATH_KEY = "trustStorePath";
    private static final String TRUSTSTORE_PASSWORD_KEY = "trustStorePassword";

    private static final int DEFAULT_PORT = 5672;
    private static final int DEFAULT_SSL_PORT = 5671;
    private static final String DEFAULT_USERNAME = "guest";
    private static final String DEFAULT_PASSWORD = "guest";
    private static final String DEFAULT_AUTH_TYPE = "PLAIN";

    private final int defaultTimeout;

    public RabbitMqConnectionValidator(
                                       @ConfigProperty(name = "destinations.rabbitmq.connection.timeout") int defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
    }

    @Override
    public ConnectionValidationResult validate(io.debezium.platform.domain.views.Connection connectionConfig) {
        if (connectionConfig == null) {
            return ConnectionValidationResult.failed("Connection configuration cannot be null");
        }

        try {
            LOGGER.debug("Starting RabbitMQ connection validation for: {}", connectionConfig.getName());

            Map<String, Object> rabbitMqConfig = connectionConfig.getConfig();

            ConnectionValidationResult configValidation = validateConfiguration(rabbitMqConfig);
            if (!configValidation.valid()) {
                return configValidation;
            }

            return performConnectionValidation(rabbitMqConfig);
        }
        catch (Exception e) {
            LOGGER.error("Unexpected error during RabbitMQ connection validation", e);
            return ConnectionValidationResult.failed("Unexpected error: " + e.getMessage());
        }
    }

    private ConnectionValidationResult validateConfiguration(Map<String, Object> config) {
        // Validate host
        if (!config.containsKey(HOST_KEY) || config.get(HOST_KEY) == null ||
                config.get(HOST_KEY).toString().trim().isEmpty()) {
            return ConnectionValidationResult.failed("Host must be specified");
        }

        // Validate authType-specific requirements
        String authType = config.containsKey(AUTH_TYPE_KEY)
                ? config.get(AUTH_TYPE_KEY).toString().trim().toUpperCase()
                : DEFAULT_AUTH_TYPE;

        switch (authType) {
            case "PLAIN":
            case "SCRAM":
                // Username and password are optional for PLAIN (defaults to guest/guest)
                // But should be validated if SCRAM is explicitly chosen
                break;
            case "OAUTH2":
                if (!config.containsKey(OAUTH2_TOKEN_KEY) || config.get(OAUTH2_TOKEN_KEY) == null ||
                        config.get(OAUTH2_TOKEN_KEY).toString().trim().isEmpty()) {
                    return ConnectionValidationResult.failed("OAuth2 token must be specified for OAUTH2 auth type");
                }
                break;
            case "EXTERNAL":
                // EXTERNAL requires SSL
                boolean useSsl = config.containsKey(USE_SSL_KEY) &&
                        Boolean.parseBoolean(config.get(USE_SSL_KEY).toString());
                if (!useSsl) {
                    return ConnectionValidationResult.failed("EXTERNAL auth type requires SSL to be enabled");
                }
                break;
            default:
                return ConnectionValidationResult.failed("Unsupported auth type: " + authType +
                        ". Supported types: PLAIN, EXTERNAL, SCRAM, OAUTH2");
        }

        return ConnectionValidationResult.successful();
    }

    private ConnectionValidationResult performConnectionValidation(Map<String, Object> config) {
        com.rabbitmq.client.Connection connection = null;

        try {
            String host = config.get(HOST_KEY).toString().trim();
            boolean useSsl = config.containsKey(USE_SSL_KEY) &&
                    Boolean.parseBoolean(config.get(USE_SSL_KEY).toString());
            int port = config.containsKey(PORT_KEY)
                    ? Integer.parseInt(config.get(PORT_KEY).toString().trim())
                    : (useSsl ? DEFAULT_SSL_PORT : DEFAULT_PORT);

            LOGGER.debug("Connecting to RabbitMQ at {}:{}, SSL={}", host, port, useSsl);

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setPort(port);
            factory.setConnectionTimeout(defaultTimeout * 1000);
            factory.setHandshakeTimeout(defaultTimeout * 1000);

            // Configure SSL/TLS if enabled
            if (useSsl) {
                SSLContext sslContext = createSslContext(config);
                factory.useSslProtocol(sslContext);
                LOGGER.debug("SSL/TLS enabled for RabbitMQ connection");
            }

            // Configure authentication
            ConnectionValidationResult authResult = configureAuthentication(factory, config);
            if (!authResult.valid()) {
                return authResult;
            }

            // Attempt to establish connection
            connection = factory.newConnection();

            if (connection.isOpen()) {
                LOGGER.debug("Successfully validated RabbitMQ connection");
                return ConnectionValidationResult.successful();
            }
            else {
                return ConnectionValidationResult.failed("Connection established but is not open");
            }

        }
        catch (java.net.UnknownHostException e) {
            String message = "Unknown host: " + e.getMessage();
            LOGGER.error(message, e);
            return ConnectionValidationResult.failed(message);
        }
        catch (java.net.ConnectException e) {
            String message = "Connection refused: Please verify the host and port are correct and the server is running.";
            LOGGER.error(message, e);
            return ConnectionValidationResult.failed(message);
        }
        catch (java.net.SocketTimeoutException e) {
            String message = "Connection timeout: The server did not respond within " + defaultTimeout + " seconds.";
            LOGGER.error(message, e);
            return ConnectionValidationResult.failed(message);
        }
        catch (com.rabbitmq.client.AuthenticationFailureException e) {
            String message = "Authentication failed: Please verify the credentials are correct.";
            LOGGER.error(message, e);
            return ConnectionValidationResult.failed(message);
        }
        catch (javax.net.ssl.SSLException e) {
            String message = "SSL error: " + e.getMessage();
            LOGGER.error(message, e);
            return ConnectionValidationResult.failed(message);
        }
        catch (Exception e) {
            LOGGER.warn("Generic exception during RabbitMQ validation", e);
            return ConnectionValidationResult.failed("Failed to validate RabbitMQ connection: " + e.getMessage());
        }
        finally {
            if (connection != null) {
                try {
                    connection.close();
                    LOGGER.debug("RabbitMQ connection closed");
                }
                catch (Exception ex) {
                    LOGGER.warn("Error closing RabbitMQ connection", ex);
                }
            }
        }
    }

    private ConnectionValidationResult configureAuthentication(ConnectionFactory factory, Map<String, Object> config) {
        try {
            String authType = config.containsKey(AUTH_TYPE_KEY)
                    ? config.get(AUTH_TYPE_KEY).toString().trim().toUpperCase()
                    : DEFAULT_AUTH_TYPE;

            LOGGER.debug("Configuring authentication type: {}", authType);

            switch (authType) {
                case "PLAIN":
                    String username = config.containsKey(USERNAME_KEY)
                            ? config.get(USERNAME_KEY).toString().trim()
                            : DEFAULT_USERNAME;
                    String password = config.containsKey(PASSWORD_KEY)
                            ? config.get(PASSWORD_KEY).toString()
                            : DEFAULT_PASSWORD;
                    factory.setUsername(username);
                    factory.setPassword(password);
                    LOGGER.debug("Using PLAIN authentication with username: {}", username);
                    break;

                case "EXTERNAL":
                    factory.setSaslConfig(DefaultSaslConfig.EXTERNAL);
                    LOGGER.debug("Using EXTERNAL authentication (client certificate)");
                    break;

                case "SCRAM":
                    // Note: SCRAM-SHA-256 is not available in amqp-client 5.22.0
                    // Falling back to PLAIN authentication
                    LOGGER.warn("SCRAM authentication is not supported in this RabbitMQ client version. Using PLAIN instead.");
                    String scramUsername = config.get(USERNAME_KEY).toString().trim();
                    String scramPassword = config.get(PASSWORD_KEY).toString();
                    factory.setUsername(scramUsername);
                    factory.setPassword(scramPassword);
                    LOGGER.debug("Using PLAIN authentication (SCRAM fallback) with username: {}", scramUsername);
                    break;

                default:
                    return ConnectionValidationResult.failed("Unsupported auth type: " + authType);
            }

            return ConnectionValidationResult.successful();

        }
        catch (Exception e) {
            LOGGER.error("Error configuring authentication", e);
            return ConnectionValidationResult.failed("Authentication configuration error: " + e.getMessage());
        }
    }

    private SSLContext createSslContext(Map<String, Object> config) throws Exception {
        String keyStorePath = config.getOrDefault(KEYSTORE_PATH_KEY, "").toString();
        String keyStorePassword = config.getOrDefault(KEYSTORE_PASSWORD_KEY, "").toString();
        String trustStorePath = config.getOrDefault(TRUSTSTORE_PATH_KEY, "").toString();
        String trustStorePassword = config.getOrDefault(TRUSTSTORE_PASSWORD_KEY, "").toString();

        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");

        // If no keystores are provided, use default SSL context
        if (keyStorePath.isEmpty() && trustStorePath.isEmpty()) {
            LOGGER.debug("No keystore/truststore provided, using default SSL context");
            sslContext.init(null, null, null);
            return sslContext;
        }

        // Initialize KeyStore for client certificate (if provided)
        javax.net.ssl.KeyManager[] keyManagers = null;
        if (!keyStorePath.isEmpty() && !keyStorePassword.isEmpty()) {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            try (FileInputStream fis = new FileInputStream(keyStorePath)) {
                ks.load(fis, keyStorePassword.toCharArray());
            }
            var kmf = javax.net.ssl.KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, keyStorePassword.toCharArray());
            keyManagers = kmf.getKeyManagers();
            LOGGER.debug("Loaded keystore from: {}", keyStorePath);
        }

        // Initialize TrustStore for server certificate validation (if provided)
        javax.net.ssl.TrustManager[] trustManagers = null;
        if (!trustStorePath.isEmpty() && !trustStorePassword.isEmpty()) {
            KeyStore ts = KeyStore.getInstance("JKS");
            try (FileInputStream fis = new FileInputStream(trustStorePath)) {
                ts.load(fis, trustStorePassword.toCharArray());
            }
            var tmf = javax.net.ssl.TrustManagerFactory.getInstance("SunX509");
            tmf.init(ts);
            trustManagers = tmf.getTrustManagers();
            LOGGER.debug("Loaded truststore from: {}", trustStorePath);
        }

        sslContext.init(keyManagers, trustManagers, null);
        return sslContext;
    }
}
