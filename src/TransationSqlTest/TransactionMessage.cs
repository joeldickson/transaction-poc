using System.Threading.Tasks;

namespace TransactionSqlTest;

[Serializable]
public class TransactionMessage
{
    public string Id { get; set; }
    public decimal Amount { get; set; }
}