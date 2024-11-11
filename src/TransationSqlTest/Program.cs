using Npgsql;
using System.Data;
using Confluent.Kafka;
using KafkaFlow;
using Acks = KafkaFlow.Acks;
using System.Runtime.CompilerServices;
using KafkaFlow.Serializer;
using TransactionSqlTest;
[assembly: InternalsVisibleTo("TransactionSqlTest.Tests")]

var builder = WebApplication.CreateBuilder(args);

// Add PostgreSQL
builder.Services.AddTransient<IDbConnection>(_ =>
    new NpgsqlConnection(builder.Configuration.GetConnectionString("PostgreSQL")));

// Add KafkaFlow
builder.Services.AddKafka(kafka => kafka
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

builder.Services.AddControllers();

var app = builder.Build();

app.MapControllers();

// Initialize KafkaFlow
var kafkaBus = app.Services.CreateKafkaBus();
await kafkaBus.StartAsync();
app.Run();
