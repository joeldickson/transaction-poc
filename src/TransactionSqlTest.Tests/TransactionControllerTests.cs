using System.Data;
using System.Net.Http.Json;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using DotNet.Testcontainers.Builders;
using KafkaFlow;
using KafkaFlow.Serializer;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;
using NUnit.Framework;
using Shouldly;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;
using Acks = KafkaFlow.Acks;

namespace TransactionSqlTest.Tests;

[TestFixture]
public class TransactionControllerTests
{
    private readonly PostgreSqlContainer _postgresContainer;
    private readonly KafkaContainer _kafkaContainer;
    private WebApplicationFactory<Program> _factory;

    public TransactionControllerTests()
    {

        _postgresContainer = new PostgreSqlBuilder()
            .WithImage("postgres:15-alpine")
            .WithDatabase("testdb")
            .WithUsername("test")
            .WithPassword("test")
            .WithPortBinding(5432)
            //.WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(5432))
            .Build();

        _kafkaContainer = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:latest")
            //.WithName("kafka-" + Guid.NewGuid())
            .WithPortBinding(9092)
            .WithEnvironment("KAFKA_BROKER_ID", "1")
            .WithEnvironment("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
            .WithEnvironment("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092")
            .WithEnvironment("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092")
            .WithEnvironment("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT")
            .WithEnvironment("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
            .WithEnvironment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .WithEnvironment("KAFKA_CREATE_TOPICS", "transaction-topic:1:1")
            //.WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9092))
            .Build();

    }
    [OneTimeSetUp]
    public async Task InitializeAsync()
    {
        _factory = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder =>
            {
                builder.ConfigureServices(services =>
                {
                    // Replace PostgreSQL connection
                    services.RemoveAll<IDbConnection>();
                    services.AddTransient<IDbConnection>(_ =>
                        new NpgsqlConnection(_postgresContainer.GetConnectionString()));

                    // Replace Kafka configuration
                    services.AddKafka(kafka => kafka
                        .UseConsoleLog()
                        .AddCluster(cluster => cluster
                            .WithBrokers(new[] { "localhost:9092" })
                            .AddProducer<TransactionMessage>(
                                producer => producer
                                    .AddMiddlewares(m =>
                                        m.AddSerializer<NewtonsoftJsonSerializer>()
                                    )
                            )
                        )
                    );
                });
            });
        await _postgresContainer.StartAsync();
        await _kafkaContainer.StartAsync();

        // Create test database schema
        using var connection = new NpgsqlConnection(_postgresContainer.GetConnectionString());
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE transactions (
                id TEXT PRIMARY KEY,
                amount DECIMAL NOT NULL,
                status TEXT NOT NULL
            )";
        await command.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        await _postgresContainer.DisposeAsync();
        await _kafkaContainer.DisposeAsync();
        await _factory.DisposeAsync();
    }

    [Test]
    public async Task ProcessTransaction_ShouldInsertDataAndProduceKafkaMessage()
    {
        // Arrange
        var client = _factory.CreateClient();
        var request = new TransactionRequest
        {
            Id = Guid.NewGuid().ToString(),
            Amount = 100.00m
        };

        // Act
        var response = await client.PostAsJsonAsync("/transaction", request);

        // Assert
        response.EnsureSuccessStatusCode();

        // Verify PostgreSQL insert
        using var connection = new NpgsqlConnection(_postgresContainer.GetConnectionString());
        await connection.OpenAsync();

        var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM transactions WHERE id = @Id";
        command.Parameters.Add(new NpgsqlParameter("@Id", request.Id));

        var count = await command.ExecuteScalarAsync();
        Convert.ToInt32(count).ShouldBe(1);

        // Optional: Add Kafka message verification
        // This would require setting up a consumer to verify the message
    }

    // Additional test to verify Kafka message production
    [Test]
    public async Task ProcessTransaction_ShouldProduceKafkaMessage()
    {
        // Arrange
        var client = _factory.CreateClient();
        var request = new TransactionRequest
        {
            Id = Guid.NewGuid().ToString(),
            Amount = 100.00m
        };

        // Setup Kafka consumer
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = $"localhost:{_kafkaContainer.GetMappedPublicPort(9092)}",
            GroupId = "test-consumer",
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<string, TransactionMessage>(consumerConfig)
            .SetValueDeserializer(new JsonDeserializer<TransactionMessage>().AsSyncOverAsync())
            .Build();
        consumer.Subscribe("transaction-topic");

        // Act
        var response = await client.PostAsJsonAsync("/transaction", request);

        // Assert
        response.EnsureSuccessStatusCode();
        Thread.Sleep(3000);
        // Wait for and verify Kafka message
        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(10));
        consumeResult.ShouldNotBeNull();
        request.Id.ShouldBe(consumeResult.Message.Value.Id);
    }
}