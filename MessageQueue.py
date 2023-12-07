
import pika



class MessageQueue:
    def __init__(self, host='localhost'):
        self.host = host
        self.connection = None
        self.channel = None

    def connect(self):
        try:
            # Establecer una conexion con el servidor rqbbitmq
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            # Crear un canal de comunicacion
            self.channel = self.connection.channel()
            return True
        except Exception as e:
            print(f"Error al conectar a RabbitMQ: {str(e)}")
            return False

    def declare_queue(self, queue_name):
        # Declarar una cola en RabbitMQ
        self.channel.queue_declare(queue=queue_name)

    def publish_message(self, queue_name, message):
        try:
            if self.connection and self.connection.is_open:
            # Publicar un mensaje en la cola especificada
                self.channel.basic_publish(exchange='', routing_key=queue_name, body=message)
            else:
                print("Error al publicar mensaje: Conexión cerrada. Intentando reconectar...")
            self.connect()  # Intentar reconectar
            if self.connection and self.connection.is_open:
                self.channel.basic_publish(exchange='', routing_key=queue_name, body=message)
            else:
                print("Error al publicar mensaje: No se pudo reconectar.")
        except Exception as e:
            print(f"Error al publicar mensaje: {str(e)}")


    def close_connection(self):
        if self.connection:
            # Cerrar la conexion con rqbbitMQ
            self.connection.close()

    def start_consuming(self, queue_name, callback):
        # Configurar un consumidor para la cola especificada y proporcionar una función de devolución de llamada
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        # Iniciar el consumo de mensajes
        self.channel.start_consuming()
