using System.Data;
using System.Text.Json;
using KafkaFlow;
using KafkaFlow.Producers;
using Microsoft.AspNetCore.Mvc;
using Npgsql;

namespace TransactionSqlTest;

[ApiController]
[Route("transaction")]
public class TransactionController : ControllerBase
{
    private readonly IDbConnection _connection;
    private readonly IMessageProducer _producer;
    private readonly string _topicName = "transaction-topic";

    public TransactionController(
        IDbConnection connection,
        IProducerAccessor producer)
    {
        _connection = connection;
        _producer = producer.GetProducer<TransactionMessage>();
    }

    [HttpPost]
    public async Task<IActionResult> ProcessTransaction([FromBody] TransactionRequest request)
    {
        await (_connection as NpgsqlConnection)!.OpenAsync();

        using var transaction = _connection.BeginTransaction(IsolationLevel.Serializable);
        try
        {
            // Insert into PostgreSQL
            CallPostgresql(request, transaction);
           
            // Send Kafka message
            SendKafkaMessage(request);
        
            transaction.Commit();
            return Ok(new { Message = "Transaction processed successfully" });
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
        finally
        {
            if (_connection.State == ConnectionState.Open)
            {
                _connection.Close();
            }
        }
    }

    private void SendKafkaMessage(TransactionRequest request)
    {
        _producer.Produce(
            _topicName,
            null,
            new TransactionMessage { Id = request.Id, Amount = request.Amount }
        );
    }

    private void CallPostgresql(TransactionRequest request, IDbTransaction transaction)
    {
        const string sql = @"
                INSERT INTO transactions (id, amount, status)
                VALUES (@Id, @Amount, @Status)";

        using var command = _connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        command.Parameters.Add(new NpgsqlParameter("@Id", request.Id));
        command.Parameters.Add(new NpgsqlParameter("@Amount", request.Amount));
        command.Parameters.Add(new NpgsqlParameter("@Status", "Pending"));

        command.ExecuteNonQuery();
    }
}