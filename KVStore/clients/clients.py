from typing import Union, Dict
import grpc
import logging
import threading

from KVStore.protos.kv_store_pb2 import GetRequest, PutRequest, GetResponse, AppendRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2 import QueryRequest, QueryResponse, QueryReplicaRequest, Operation
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterStub

logger = logging.getLogger(__name__)


def _get_return(ret: GetResponse) -> Union[str, None]:
    if ret.HasField("value"):
        return ret.value
    else:
        return None


class SimpleClient:

    def __init__(self, kvstore_address: str):
        self.channel = grpc.insecure_channel(kvstore_address)
        self.stub = KVStoreStub(self.channel)
        self.mutex = threading.Lock()

    def get(self, key: int) -> Union[str, None]:
        # Crea una instancia del mensaje y asigna valores a sus campos
        mess = GetRequest(key=key)
        # Llama al método remoto en el servidor
        self.mutex.acquire()
        val = self.stub.Get(mess)
        self.mutex.release()
        res = val.value
        if res == 'None':
            return None
        return res

    def l_pop(self, key: int) -> Union[str, None]:
        mess = GetRequest(key=key)
        self.mutex.acquire()
        val = self.stub.LPop(mess)
        self.mutex.release()
        res = val.value
        if res == 'None':
            return None
        return res

    def r_pop(self, key: int) -> Union[str, None]:
        mess = GetRequest(key=key)
        self.mutex.acquire()
        val = self.stub.RPop(mess)
        self.mutex.release()
        res = val.value
        if res == 'None':
            return None
        return res

    def put(self, key: int, value: str):
        mess = PutRequest(key=key, value=value)
        self.mutex.acquire()
        self.stub.Put(mess)
        self.mutex.release()

    def append(self, key: int, value: str):
        mess = AppendRequest(key=key, value=value)
        self.mutex.acquire()
        self.stub.Append(mess)
        self.mutex.release()

    def stop(self):
        self.channel.close()


class ShardClient(SimpleClient):
    def __init__(self, shard_master_address: str):
        self.channel = grpc.insecure_channel(shard_master_address)
        self.stub = ShardMasterStub(self.channel)
        self.stub2 = ""
        self.channel2 = ""
        self.mutex = threading.Lock()

    def get(self, key: int) -> Union[str, None]:
        mess = QueryRequest(key=key)
        val = self.stub.Query(mess)
        self.channel2 = grpc.insecure_channel(val.server)
        self.mutex.acquire()
        self.stub2 = KVStoreStub(self.channel2)
        self.mutex.release()
        mess = GetRequest(key=key)
        val = self.stub2.Get(mess)
        res = val.value
        if res == 'None':
            return None
        return res

    def l_pop(self, key: int) -> Union[str, None]:
        mess = QueryRequest(key=key)
        val = self.stub.Query(mess)
        self.channel2 = grpc.insecure_channel(val.server)
        self.mutex.acquire()
        self.stub2 = KVStoreStub(self.channel2)
        self.mutex.release()
        mess = GetRequest(key=key)
        val = self.stub2.LPop(mess)
        res = val.value

        if res == 'None':
            return None
        return res

    def r_pop(self, key: int) -> Union[str, None]:
        mess = QueryRequest(key=key)
        val = self.stub.Query(mess)
        self.channel2 = grpc.insecure_channel(val.server)
        self.mutex.acquire()
        self.stub2 = KVStoreStub(self.channel2)
        self.mutex.release()
        mess = GetRequest(key=key)
        val = self.stub2.RPop(mess)
        res = val.value

        if res == 'None':
            return None
        return res

    def put(self, key: int, value: str):
        mess = QueryRequest(key=key)
        val = self.stub.Query(mess)
        self.channel2 = grpc.insecure_channel(val.server)
        self.mutex.acquire()
        self.stub2 = KVStoreStub(self.channel2)
        self.mutex.release()
        mess = PutRequest(key=key, value=value)
        self.stub2.Put(mess)

    def append(self, key: int, value: str):
        mess = QueryRequest(key=key)
        val = self.stub.Query(mess)
        self.channel2 = grpc.insecure_channel(val.server)
        self.mutex.acquire()
        self.stub2 = KVStoreStub(self.channel2)
        self.mutex.release()
        mess = PutRequest(key=key, value=value)
        self.stub2.Append(mess)


class ShardReplicaClient(ShardClient):

    def get(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def r_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def put(self, key: int, value: str):
        """
        To fill with your code
        """

    def append(self, key: int, value: str):
        """
        To fill with your code
        """
