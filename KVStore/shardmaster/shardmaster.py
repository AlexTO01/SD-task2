import logging

import grpc

from KVStore.tests.utils import KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer
from KVStore.protos.kv_store_shardmaster_pb2 import *
import KVStore

logger = logging.getLogger(__name__)
n = range(KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD)


class server_struct:
    def __init__(self, server: str):
        self.server = server
        self.maxkeys = 0
        self.minkeys = 0

    def asignarKey(self, maxkey: int, minkey: int):
        self.minkeys = minkey
        self.maxkeys = maxkey

    def getServer(self) -> str:
        return self.server

    def getKeys(self) -> range:
        return range(self.minkeys, self.maxkeys)

    def getMinKey(self) -> int:
        return self.minkeys

    def getMaxKey(self) -> int:
        return self.maxkeys

    def show(self) -> str:
        val = "Server: " + str(self.server) + " have keys: " + str(self.minkeys) + "-" + str(self.maxkeys)
        return val

    def restartKeys(self):
        self.maxkeys = 0
        self.minkeys = 0


class ShardMasterService:
    def join(self, server: str):
        pass

    def leave(self, server: str):
        pass

    def query(self, key: int) -> str:
        pass

    def join_replica(self, server: str) -> Role:
        pass

    def query_replica(self, key: int, op: Operation) -> str:
        pass


class ShardMasterSimpleService(ShardMasterService):
    def __init__(self):
        self.servers = []

    def join(self, server: str):
        serv = server_struct(server)
        for server in self.servers:
            server.restartKeys()

        self.servers.append(serv)
        self.distribuir()

    def leave(self, server: str):
        for serv in self.servers:
            if server == serv.getServer():
                self.servers.remove(serv)
                if len(self.servers) != 0:
                    self.distribuir()

    def query(self, key: int) -> str:
        for serv in self.servers:
            if key in serv.getKeys():
                return serv.getServer()

    def distribuir(self):
        keys_por_servidor = len(n) // len(self.servers)  # Número de keys por servidor (división entera)
        sobrante = len(n) % len(self.servers)  # Número de keys sobrantes
        inicio = 0

        for i in range(len(self.servers)):
            fin = inicio + keys_por_servidor

            # Si hay keys sobrantes, distribuirlas equitativamente
            if sobrante > 0:
                fin += 1
                sobrante -= 1
            self.servers[i].asignarKey(fin, inicio)

            inicio = fin  # Actualizar el inicio para el siguiente servidor

        for server in self.servers:
            channel = grpc.insecure_channel(server.getServer())
            stub = KVStoreStub(channel)
            mess = RedistributeRequest(destination_server=server.getServer(), lower_val=server.getMinKey(),
                                       upper_val=server.getMaxKey())
            stub.Redistribute(mess)


class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        """
        To fill with your code
        """

    def leave(self, server: str):
        """
        To fill with your code
        """

    def join_replica(self, server: str) -> Role:
        """
        To fill with your code
        """

    def query_replica(self, key: int, op: Operation) -> str:
        """
        To fill with your code
        """


class ShardMasterServicer(ShardMasterServicer):
    def __init__(self, shard_master_service: ShardMasterService):
        self.shard_master_service = shard_master_service

    def Join(self, request: JoinRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        serv = request.server
        self.shard_master_service.join(serv)
        return KVStore.protos.kv_store_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()

    def Leave(self, request: LeaveRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        serv = request.server
        self.shard_master_service.leave(serv)
        return KVStore.protos.kv_store_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()

    def Query(self, request: QueryRequest, context) -> QueryResponse:
        query = request.key
        resp = self.shard_master_service.query(query)
        mess = QueryResponse(server=resp)
        return mess

    def JoinReplica(self, request: JoinRequest, context) -> JoinReplicaResponse:
        """
        To fill with your code
        """

    def QueryReplica(self, request: QueryReplicaRequest, context) -> QueryResponse:
        """
        To fill with your code
        """
