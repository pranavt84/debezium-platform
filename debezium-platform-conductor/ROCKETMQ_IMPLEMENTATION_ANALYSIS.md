# RocketMQ Connection Validator - Implementation Analysis

> **Status**: Analysis Phase - DO NOT IMPLEMENT YET
> 
> This document analyzes the Kinesis implementation pattern to replicate it for RocketMQ.

---

## 📋 Table of Contents

1. [File Structure Analysis](#file-structure-analysis)
2. [Kinesis Implementation Pattern](#kinesis-implementation-pattern)
3. [RocketMQ Requirements](#rocketmq-requirements)
4. [Implementation Checklist](#implementation-checklist)
5. [Configuration Details](#configuration-details)
6. [Testing Strategy](#testing-strategy)

---

## 1. File Structure Analysis

### Existing Kinesis Files

```
debezium-platform-conductor/
├── src/main/java/.../destination/
│   └── AmazonKinesisConnectionValidator.java         ← Main validator class
├── src/test/java/.../connection/
│   ├── AmazonKinesisConnectionValidatorTest.java     ← Unit tests (19 tests)
│   └── AmazonKinesisConnectionValidatorIT.java       ← Integration tests (17 tests)
├── src/test/java/.../database/db/
│   └── AmazonKinesisTestResource.java                ← Testcontainers resource
├── KINESIS_TESTING_GUIDE.md                          ← Testing documentation
└── KINESIS_TEST_COVERAGE.md                          ← Coverage documentation
```

### Required RocketMQ Files (to create)

```
debezium-platform-conductor/
├── src/main/java/.../destination/
│   └── RocketMqConnectionValidator.java              ← NEEDS CREATION
├── src/test/java/.../connection/
│   ├── RocketMqConnectionValidatorTest.java          ← NEEDS CREATION
│   └── RocketMqConnectionValidatorIT.java            ← NEEDS CREATION
├── src/test/java/.../database/db/
│   └── RocketMqTestResource.java                     ← NEEDS CREATION
├── ROCKETMQ_TESTING_GUIDE.md                         ← NEEDS CREATION
└── ROCKETMQ_TEST_COVERAGE.md                         ← NEEDS CREATION
```

---

## 2. Kinesis Implementation Pattern

### 2.1 Main Validator Class Structure

**File**: `AmazonKinesisConnectionValidator.java`

```java
@ApplicationScoped
@Named("AMAZON_KINESIS")  // ← Maps to ConnectionEntity.Type.AMAZON_KINESIS
public class AmazonKinesisConnectionValidator implements ConnectionValidator {
    
    // 1. Constants for configuration keys
    private static final String REGION_KEY = "region";
    private static final String STREAM_NAME_KEY = "stream";
    private static final String ENDPOINT_KEY = "endpoint";  // Optional, for LocalStack
    
    // 2. Timeout configuration from application.yml
    @ConfigProperty(name = "destinations.kinesis.connection.timeout")
    private final int defaultTimeout;
    
    // 3. Constructor injection
    public AmazonKinesisConnectionValidator(@ConfigProperty(...) int defaultTimeout) {
        this.defaultTimeout = defaultTimeout;
    }
    
    // 4. Main validation method
    @Override
    public ConnectionValidationResult validate(Connection connectionConfig) {
        // - Null check
        // - Call validateConfiguration()
        // - Call performConnectionValidation()
        // - Exception handling
    }
    
    // 5. Configuration validation (required fields)
    private ConnectionValidationResult validateConfiguration(Map<String, Object> config) {
        // Check required fields: region, stream
        // Check for null, empty, whitespace
        // Return failed() or successful()
    }
    
    // 6. Actual connection test
    private ConnectionValidationResult performConnectionValidation(Map<String, Object> config) {
        // Create client (with optional endpoint override for testing)
        // Test connectivity (describeStreamSummary)
        // Handle specific exceptions
        // Close resources
    }
}
```

**Key Patterns**:
- ✅ `@ApplicationScoped` - CDI bean scope
- ✅ `@Named("AMAZON_KINESIS")` - Matches enum type
- ✅ Timeout configured in `application.yml`
- ✅ Three-step validation: null check → config validation → connection test
- ✅ Specific exception handling (ResourceNotFound, AccessDenied, ClientError)
- ✅ Resource cleanup in `finally` block
- ✅ Optional `endpoint` field for LocalStack testing

### 2.2 Configuration in application.yml

```yaml
destinations:
  kafka:
    connection:
      timeout: 60
  kinesis:
    connection:
      timeout: 5
  # RocketMQ will need similar entry
```

**Pattern**: `destinations.<service>.connection.timeout`

### 2.3 Unit Test Structure

**File**: `AmazonKinesisConnectionValidatorTest.java`

**Test Categories** (19 tests total):
1. **Null/Empty Configuration** (1 test)
   - Null connection config

2. **Region Validation** (4 tests)
   - Missing region
   - Null region
   - Empty region
   - Whitespace-only region

3. **Stream Name Validation** (4 tests)
   - Missing stream name
   - Null stream name
   - Empty stream name
   - Whitespace-only stream name

4. **Combined Missing** (1 test)
   - Both region and stream missing

5. **Optional Fields** (1 test)
   - Missing optional endpoint field

6. **Edge Cases** (2 tests)
   - Very long stream name
   - Special characters in stream name

**Pattern**:
- Uses `@BeforeEach` to setup validator
- Each test is independent
- Clear `@DisplayName` annotations
- Organized into sections with comments
- Tests validation logic only (no real connections)

### 2.4 Integration Test Structure

**File**: `AmazonKinesisConnectionValidatorIT.java`

**Requirements**:
- `@QuarkusTest` annotation
- `@QuarkusTestResource(AmazonKinesisTestResource.class)` annotation
- `@Inject` validator
- Uses `ConfigProvider.getConfig().getValue()` to get test properties

**Test Categories** (17 tests total):
1. **Successful Connection** (3 tests)
   - Valid connection
   - Endpoint with trailing slash
   - Whitespace trimming

2. **Stream Not Found** (2 tests)
   - Non-existent stream
   - Descriptive error message

3. **Invalid Region** (1 test)
   - Empty region

4. **Missing Configuration** (3 tests)
   - Missing region
   - Missing stream name
   - Both missing

5. **Endpoint Tests** (3 tests)
   - Without endpoint (tries real AWS)
   - Malformed endpoint
   - Trailing slash in endpoint

6. **Optional Fields** (2 tests)
   - Without partitionKey
   - With partitionKey

7. **Edge Cases** (2 tests)
   - Special characters in stream name
   - Null connection config

**Pattern**:
- Tests against real LocalStack container
- Tests both success and failure scenarios
- Validates error messages
- Tests optional field handling

### 2.5 Test Resource (Testcontainers)

**File**: `AmazonKinesisTestResource.java`

**Structure**:
```java
public class AmazonKinesisTestResource implements QuarkusTestResourceLifecycleManager {
    
    private LocalStackContainer localStack;
    private final String streamName = "test-stream";
    
    @Override
    public Map<String, String> start() {
        // 1. Start LocalStack container with service
        // 2. Get endpoint and region
        // 3. Create client with dummy credentials
        // 4. Create test stream
        // 5. Wait for stream to become ACTIVE
        // 6. Return config properties
    }
    
    @Override
    public void stop() {
        // Stop container
    }
    
    private void waitForStreamToBecomeActive(...) {
        // Poll status until ACTIVE or timeout
    }
}
```

**Key Points**:
- Uses LocalStack for AWS emulation
- Creates resources before tests run
- Returns config properties as Map
- Waits for resources to be ready
- Cleans up after tests

---

## 3. RocketMQ Requirements

### 3.1 What is RocketMQ?

- **Apache RocketMQ**: Distributed messaging and streaming platform
- **Architecture**: NameServer + Broker + Producer + Consumer
- **Key Concepts**:
  - **NameServer**: Service discovery/routing
  - **Broker**: Message storage/delivery
  - **Topic**: Message category
  - **Producer Group**: Producer cluster
  - **Consumer Group**: Consumer cluster

### 3.2 RocketMQ Connection Parameters

**Required Fields**:
- `namesrvAddr` - NameServer address (e.g., "localhost:9876")
- `topic` - Topic name to validate

**Optional Fields**:
- `accessKey` - ACL access key (if security enabled)
- `secretKey` - ACL secret key (if security enabled)
- `namespace` - Instance namespace
- `producerGroup` - Producer group name (default: "DEFAULT_PRODUCER")

### 3.3 RocketMQ Validation Strategy

**What to validate**:
1. NameServer connectivity
2. Topic existence and accessibility
3. Credentials (if provided)

**How to validate**:
- Create a `DefaultMQProducer` instance
- Start the producer
- Fetch topic route info (validates topic exists)
- Shutdown producer

### 3.4 RocketMQ Java Client

**Maven Dependency**:
```xml
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-client</artifactId>
    <version>5.x.x</version>  <!-- Check latest version -->
</dependency>
```

**Key Classes**:
- `org.apache.rocketmq.client.producer.DefaultMQProducer`
- `org.apache.rocketmq.client.exception.MQClientException`
- `org.apache.rocketmq.client.exception.MQBrokerException`
- `org.apache.rocketmq.common.message.Message`

### 3.5 RocketMQ Test Container

**Testcontainers**:
- **Image**: `apache/rocketmq:latest` or `rocketmqinc/rocketmq:latest`
- **Ports**: 
  - 9876 (NameServer)
  - 10911 (Broker VIP)
  - 10909 (Broker)
- **Environment Variables**:
  - `NAMESRV_ADDR` - NameServer address
  
**Important**: RocketMQ requires both NameServer and Broker to run properly.

**Docker Compose Alternative** (simpler for testing):
```yaml
version: '3.8'
services:
  namesrv:
    image: apache/rocketmq:latest
    container_name: rmqnamesrv
    ports:
      - 9876:9876
    command: sh mqnamesrv
  
  broker:
    image: apache/rocketmq:latest
    container_name: rmqbroker
    ports:
      - 10909:10909
      - 10911:10911
    depends_on:
      - namesrv
    environment:
      - NAMESRV_ADDR=namesrv:9876
    command: sh mqbroker -n namesrv:9876 -c /etc/rocketmq/broker.conf
```

---

## 4. Implementation Checklist

### Phase 1: Dependencies & Configuration

- [ ] Add RocketMQ client dependency to `pom.xml`
- [ ] Add RocketMQ Testcontainers support (or custom container management)
- [ ] Add timeout configuration to `application.yml`:
  ```yaml
  destinations:
    rocketmq:
      connection:
        timeout: 30
  ```
- [ ] Verify `ConnectionEntity.Type.APACHE_ROCKETMQ` exists ✅ (Already exists at line 57)

### Phase 2: Main Validator Class

- [ ] Create `RocketMqConnectionValidator.java`
  - [ ] Add `@ApplicationScoped` annotation
  - [ ] Add `@Named("APACHE_ROCKETMQ")` annotation
  - [ ] Define configuration key constants:
    - [ ] `NAMESRV_ADDR_KEY = "namesrvAddr"`
    - [ ] `TOPIC_KEY = "topic"`
    - [ ] `ACCESS_KEY = "accessKey"` (optional)
    - [ ] `SECRET_KEY = "secretKey"` (optional)
    - [ ] `NAMESPACE_KEY = "namespace"` (optional)
    - [ ] `PRODUCER_GROUP_KEY = "producerGroup"` (optional)
  - [ ] Inject timeout from config: `@ConfigProperty(name = "destinations.rocketmq.connection.timeout")`
  - [ ] Implement `validate()` method with null check
  - [ ] Implement `validateConfiguration()` method
    - [ ] Check namesrvAddr (required, not null/empty/whitespace)
    - [ ] Check topic (required, not null/empty/whitespace)
  - [ ] Implement `performConnectionValidation()` method
    - [ ] Create `DefaultMQProducer`
    - [ ] Set NameServer address
    - [ ] Set credentials if provided
    - [ ] Start producer
    - [ ] Fetch topic route info to validate
    - [ ] Shutdown producer in finally block
  - [ ] Exception handling:
    - [ ] `MQClientException` - Connection/client errors
    - [ ] `MQBrokerException` - Broker errors (topic not found, etc.)
    - [ ] Generic `Exception` - Unexpected errors

### Phase 3: Unit Tests

- [ ] Create `RocketMqConnectionValidatorTest.java`
  - [ ] Setup validator in `@BeforeEach`
  - [ ] **Null/Empty Configuration Tests** (1 test)
    - [ ] Null connection config
  - [ ] **NameServer Address Validation** (4 tests)
    - [ ] Missing namesrvAddr
    - [ ] Null namesrvAddr
    - [ ] Empty namesrvAddr
    - [ ] Whitespace-only namesrvAddr
  - [ ] **Topic Validation** (4 tests)
    - [ ] Missing topic
    - [ ] Null topic
    - [ ] Empty topic
    - [ ] Whitespace-only topic
  - [ ] **Combined Missing** (1 test)
    - [ ] Both namesrvAddr and topic missing
  - [ ] **Optional Fields** (1-2 tests)
    - [ ] Missing optional fields (accessKey, secretKey, namespace, producerGroup)
  - [ ] **Edge Cases** (2-3 tests)
    - [ ] Very long topic name
    - [ ] Special characters in topic name
    - [ ] Invalid namesrvAddr format

### Phase 4: Integration Tests

- [ ] Create `RocketMqTestResource.java`
  - [ ] Implement `QuarkusTestResourceLifecycleManager`
  - [ ] Start RocketMQ NameServer container
  - [ ] Start RocketMQ Broker container
  - [ ] Create test topic using admin client
  - [ ] Wait for broker to be ready
  - [ ] Return config properties:
    - [ ] `rocketmq.namesrvAddr`
    - [ ] `rocketmq.topic`
  - [ ] Stop containers in `stop()` method

- [ ] Create `RocketMqConnectionValidatorIT.java`
  - [ ] Add `@QuarkusTest` annotation
  - [ ] Add `@QuarkusTestResource(RocketMqTestResource.class)` annotation
  - [ ] Inject validator
  - [ ] **Successful Connection Tests** (2-3 tests)
    - [ ] Valid connection
    - [ ] Whitespace trimming
    - [ ] Optional fields provided
  - [ ] **Topic Not Found Tests** (1-2 tests)
    - [ ] Non-existent topic
    - [ ] Descriptive error message
  - [ ] **Missing Configuration Tests** (3 tests)
    - [ ] Missing namesrvAddr
    - [ ] Missing topic
    - [ ] Both missing
  - [ ] **Optional Fields Tests** (2-3 tests)
    - [ ] Without optional fields
    - [ ] With credentials (if ACL supported in test container)
    - [ ] With namespace
  - [ ] **Edge Cases** (2 tests)
    - [ ] Special characters in topic name
    - [ ] Null connection config

### Phase 5: Documentation

- [ ] Create `ROCKETMQ_TESTING_GUIDE.md`
  - [ ] Overview of RocketMQ validation
  - [ ] Unit testing approach
  - [ ] Integration testing with Testcontainers
  - [ ] Testing with real RocketMQ cluster
  - [ ] Configuration reference
  - [ ] Troubleshooting guide

- [ ] Create `ROCKETMQ_TEST_COVERAGE.md`
  - [ ] Test scenario breakdown
  - [ ] Coverage metrics
  - [ ] Methods covered
  - [ ] Error messages tested

### Phase 6: Testing & Validation

- [ ] Run unit tests: `mvn test -Dtest=RocketMqConnectionValidatorTest`
- [ ] Run integration tests: `mvn test -Dtest=RocketMqConnectionValidatorIT`
- [ ] Verify all tests pass
- [ ] Check code coverage
- [ ] Manual testing with real RocketMQ (optional)

---

## 5. Configuration Details

### 5.1 Configuration Keys Mapping

| Kinesis | RocketMQ | Required | Description |
|---------|----------|----------|-------------|
| `region` | `namesrvAddr` | ✅ Yes | AWS region → NameServer address |
| `stream` | `topic` | ✅ Yes | Stream name → Topic name |
| `endpoint` | *(not needed)* | ❌ No | LocalStack endpoint (for testing only) |
| `partitionKey` | `producerGroup` | ❌ No | Partition key → Producer group |
| *(none)* | `accessKey` | ❌ No | ACL access key |
| *(none)* | `secretKey` | ❌ No | ACL secret key |
| *(none)* | `namespace` | ❌ No | Instance namespace |

### 5.2 Error Message Mapping

| Kinesis Exception | RocketMQ Exception | Error Message |
|-------------------|-------------------|---------------|
| `ResourceNotFoundException` | Topic doesn't exist | "Topic not found: Please verify the topic name and NameServer address." |
| `AccessDeniedException` | ACL denied | "Access denied: Check ACL credentials." |
| `SdkClientException` | `MQClientException` | "Client error: {message}" |
| Generic `Exception` | Generic `Exception` | "Failed to validate RocketMQ connection: {message}" |

### 5.3 Application.yml Entry

```yaml
destinations:
  kafka:
    connection:
      timeout: 60
  kinesis:
    connection:
      timeout: 5
  rocketmq:               # ← ADD THIS
    connection:
      timeout: 30         # Reasonable default for RocketMQ
```

---

## 6. Testing Strategy

### 6.1 Unit Tests (Fast, No Dependencies)

**Goal**: Test configuration validation logic  
**Duration**: ~3-5 seconds  
**Coverage**: 15-19 tests

**What to Test**:
- ✅ Null checks
- ✅ Required field validation
- ✅ Whitespace handling
- ✅ Edge cases
- ❌ Actual RocketMQ connectivity

### 6.2 Integration Tests (Slow, Testcontainers)

**Goal**: Test actual RocketMQ connectivity  
**Duration**: ~60-90 seconds (includes container startup)  
**Coverage**: 15-17 tests

**What to Test**:
- ✅ Successful connections
- ✅ Topic not found errors
- ✅ Invalid configurations
- ✅ Optional field handling
- ✅ Error messages

### 6.3 Test Resource Strategy

**Option 1: Docker Compose (Recommended)**
- Use Docker Compose with NameServer + Broker
- More reliable than single container
- Mirrors production setup

**Option 2: Testcontainers GenericContainer**
- Use `GenericContainer` with custom setup
- Run NameServer and Broker in same container (complex)
- May require custom startup scripts

**Option 3: Testcontainers Network**
- Use Testcontainers `Network`
- Start NameServer container
- Start Broker container connected to same network
- More complex but most production-like

### 6.4 Recommended Approach

**For RocketMQ**, use **Option 3 (Testcontainers Network)**:

```java
public class RocketMqTestResource implements QuarkusTestResourceLifecycleManager {
    
    private Network network;
    private GenericContainer<?> namesrv;
    private GenericContainer<?> broker;
    
    @Override
    public Map<String, String> start() {
        // 1. Create network
        network = Network.newNetwork();
        
        // 2. Start NameServer
        namesrv = new GenericContainer<>("apache/rocketmq:latest")
            .withNetwork(network)
            .withNetworkAliases("namesrv")
            .withExposedPorts(9876)
            .withCommand("sh mqnamesrv");
        namesrv.start();
        
        // 3. Start Broker
        broker = new GenericContainer<>("apache/rocketmq:latest")
            .withNetwork(network)
            .withExposedPorts(10909, 10911)
            .withEnv("NAMESRV_ADDR", "namesrv:9876")
            .withCommand("sh mqbroker -n namesrv:9876");
        broker.start();
        
        // 4. Create test topic
        // 5. Return config
    }
}
```

---

## 7. Key Differences: Kinesis vs RocketMQ

| Aspect | Kinesis | RocketMQ |
|--------|---------|----------|
| **Service Type** | Managed AWS service | Self-hosted message queue |
| **Client Library** | AWS SDK v2 | Apache RocketMQ Client |
| **Connection String** | Region-based | NameServer address |
| **Resource Name** | Stream | Topic |
| **Authentication** | IAM/Credentials (implicit) | ACL (explicit, optional) |
| **Testing** | LocalStack (single container) | NameServer + Broker (two containers) |
| **Validation Method** | `describeStreamSummary()` | `fetchPublishMessageQueues()` or create test producer |
| **Endpoint Override** | Yes (for LocalStack) | Not needed |
| **Resource Lifecycle** | Producer auto-connects | Must explicitly start/shutdown |

---

## 8. Implementation Notes

### 8.1 RocketMQ Client Lifecycle

**Important**: RocketMQ producers must be explicitly started and shutdown:

```java
DefaultMQProducer producer = new DefaultMQProducer("test-group");
try {
    producer.setNamesrvAddr("localhost:9876");
    producer.start();  // ← Must call start()
    
    // Validate by fetching topic info
    producer.fetchPublishMessageQueues("test-topic");
    
} finally {
    producer.shutdown();  // ← Must call shutdown()
}
```

### 8.2 Topic Validation

**Best Practice**: Use `fetchPublishMessageQueues(topic)` to validate:
- Topic exists
- NameServer is reachable
- Broker is available
- Producer has access

### 8.3 ACL Testing

**Optional**: If testing with ACL enabled:
- Set `accessKey` and `secretKey` on producer
- RocketMQ ACL requires broker configuration
- May be complex for Testcontainers setup
- Consider testing ACL separately or documenting manual testing

### 8.4 Performance Considerations

- RocketMQ connection is generally faster than Kinesis
- Timeout of 30 seconds should be sufficient
- Container startup is slower (NameServer + Broker)
- Integration tests will take longer than Kinesis tests

---

## 9. Estimated Effort

| Phase | Effort | Notes |
|-------|--------|-------|
| Dependencies & Config | 30 min | Simple additions |
| Main Validator Class | 2-3 hours | Similar to Kinesis, but RocketMQ client API different |
| Unit Tests | 1-2 hours | Copy pattern from Kinesis |
| Test Resource | 2-3 hours | Most complex part - two containers |
| Integration Tests | 1-2 hours | Copy pattern from Kinesis |
| Documentation | 1-2 hours | Copy and adapt from Kinesis |
| Testing & Debugging | 2-4 hours | Container issues, timing issues |
| **Total** | **10-17 hours** | Includes learning curve and debugging |

---

## 10. Next Steps

**When ready to implement**:

1. ✅ Review this analysis
2. ✅ Confirm RocketMQ version and Docker image
3. ✅ Start with Phase 1 (Dependencies)
4. ✅ Implement phases sequentially
5. ✅ Test after each phase
6. ✅ Document as you go

**Questions to Answer Before Implementation**:

- [ ] Which RocketMQ version should we target? (4.x vs 5.x)
- [ ] Do we need to support ACL in tests?
- [ ] Do we need namespace support?
- [ ] Should we test with Docker Compose or Testcontainers Network?
- [ ] What are the most common RocketMQ connection failure scenarios to test?

---

## 11. Reference Links

- [Apache RocketMQ Documentation](https://rocketmq.apache.org/)
- [RocketMQ Java Client](https://github.com/apache/rocketmq-clients)
- [RocketMQ Docker Images](https://hub.docker.com/r/apache/rocketmq)
- [Testcontainers Documentation](https://www.testcontainers.org/)
- [Kinesis Implementation](./src/main/java/io/debezium/platform/environment/connection/destination/AmazonKinesisConnectionValidator.java)

---

**Analysis Complete** ✅  
**Ready for Implementation**: Awaiting user approval to proceed

