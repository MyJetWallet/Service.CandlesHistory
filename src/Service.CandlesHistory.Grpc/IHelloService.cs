using System.ServiceModel;
using System.Threading.Tasks;
using Service.CandlesHistory.Grpc.Models;

namespace Service.CandlesHistory.Grpc
{
    [ServiceContract]
    public interface IHelloService
    {
        [OperationContract]
        Task<HelloMessage> SayHelloAsync(HelloRequest request);
    }
}