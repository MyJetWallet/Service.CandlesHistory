using System.Runtime.Serialization;
using Service.CandlesHistory.Domain.Models;

namespace Service.CandlesHistory.Grpc.Models
{
    [DataContract]
    public class HelloMessage : IHelloMessage
    {
        [DataMember(Order = 1)]
        public string Message { get; set; }
    }
}