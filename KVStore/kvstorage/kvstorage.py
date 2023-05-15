import time
import random
from typing import Dict, Union, List
import logging
import grpc
import redis


import KVStore.protos.kv_store_pb2_grpc
from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreServicer, KVStoreStub

from KVStore.protos.kv_store_shardmaster_pb2 import Role

EVENTUAL_CONSISTENCY_INTERVAL: int = 2

logger = logging.getLogger("KVStore")


class KVStorageService:

    def __init__(self):
        pass

    def get(self, key: int) -> str:
        pass

    def l_pop(self, key: int) -> str:
        pass

    def r_pop(self, key: int) -> str:
        pass

    def put(self, key: int, value: str):
        pass

    def append(self, key: int, value: str):
        pass

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        pass

    def transfer(self, keys_values: list):
        pass

    def add_replica(self, server: str):
        pass

    def remove_replica(self, server: str):
        pass


class KVStorageSimpleService(KVStorageService):

    def __init__(self):
        super().__init__()
        self.db = redis.StrictRedis(host="localhost", port="6379", db=0, decode_responses=True)

    def get(self, key: int) -> Union[str, None]:
        val = self.db.get(key)
        if val is None:
            return 'None'
        return val

    def l_pop(self, key: int) -> Union[str, None]:
        val = self.db.get(key)
        if val is None:
            return 'None'
        elif val == "":
            return ""
        else:
            finalval = val[0]
            new = val[1:]
            self.db.set(key, new)
            return finalval

    def r_pop(self, key: int) -> Union[str, None]:
        val = self.db.get(key)
        if val is None:
            return 'None'
        elif val == "":
            return ""
        else:
            finalval = val[-1]
            new = val[:-1]
            self.db.set(key, new)
            return finalval

    def put(self, key: int, value: str):
        self.db.set(key, value)

    def append(self, key: int, value: str):

        val = self.db.get(key)

        if val is None:
            self.db.set(key, value)
        else:
            self.db.append(key, value)

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        """
        To fill with your code
        """

    def transfer(self, keys_values: List[KeyValue]):
        """
        To fill with your code
        """


class KVStorageReplicasService(KVStorageSimpleService):
    role: Role

    def __init__(self, consistency_level: int):
        super().__init__()
        self.consistency_level = consistency_level
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> str:
        """
        To fill with your code
        """

    def r_pop(self, key: int) -> str:
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

    def add_replica(self, server: str):
        """
        To fill with your code
        """

    def remove_replica(self, server: str):
        """
        To fill with your code
        """

    def set_role(self, role: Role):
        logger.info(f"Got role {role}")
        self.role = role


class KVStorageServicer(KVStoreServicer):

    def __init__(self, service: KVStorageService):
        self.storage_service = service
        """
        To fill with your code
        """

    def Get(self, request: GetRequest, context) -> GetResponse:
        key = request.key
        resp = self.storage_service.get(key)
        mess = GetResponse(value=resp)
        return mess

    def LPop(self, request: GetRequest, context) -> GetResponse:
        key = request.key
        resp = self.storage_service.l_pop(key)
        mess = GetResponse(value=resp)
        return mess

    def RPop(self, request: GetRequest, context) -> GetResponse:
        key = request.key
        resp = self.storage_service.r_pop(key)
        mess = GetResponse(value=resp)
        return mess

    def Put(self, request: PutRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        key = request.key
        val = request.value
        self.storage_service.put(key=key, value=val)
        return KVStore.protos.kv_store_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()

    def Append(self, request: AppendRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        key = request.key
        val = request.value
        self.storage_service.append(key=key, value=val)
        return KVStore.protos.kv_store_pb2_grpc.google_dot_protobuf_dot_empty__pb2.Empty()

    def Redistribute(self, request: RedistributeRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def Transfer(self, request: TransferRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def AddReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def RemoveReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """
