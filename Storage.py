import json
from flask import Flask, request, jsonify
from threading import Lock, Thread
import logging
import requests
import time

app = Flask(__name__)
lock = Lock()
data_folder = "data"
replicas = [1, 2, 3]  # Lista de IDs de las replicas

class StorageNode:
    def __init__(self, node_id, is_leader=False):
        self.node_id = node_id
        self.is_leader = is_leader
        self.log = []
        self.data = {}

    def write_operation(self, operation):
        with lock:
            # Anadir la operacion al registro de operaciones en disco y actualizar los datos
            self.log.append(operation)
            operation_type = operation.get("type")
            if operation_type == "add":
                self.data[str(operation.get("id"))] = operation.get("formulario")
            elif operation_type == "delete":
                del self.data[str(operation.get("id"))]

            # Logica para replicar la operacion a otros nodos (seguidores)
            self.replicate_operation(operation)

    def replicate_operation(self, operation):
        if self.is_leader:
            # Logica para replicar la operacion a nodos seguidores
            for replica_id in replicas:
                if replica_id != self.node_id:
                    # Logica para enviar la operacion a la replica con ID replica_id
                    self.send_operation_to_replica(replica_id, operation)

    def send_operation_to_replica(self, replica_id, operation):
        replica_address = f"http://localhost:{5000 + replica_id}/add"
        response = requests.post(replica_address, json=operation)
        logging.info(f"Réplica {replica_id}: {response.json()['message']}")



if __name__ == "__main__":
    # Configuracion de logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Inicializar el sistema de almacenamiento
    inicializar_almacenamiento()

    # Iniciar el hilo de replicacion
    replication_thread = Thread(target=replication_worker)
    replication_thread.start()

    # Ejecutar la aplicacion Flask
    app.run(port=5000)
