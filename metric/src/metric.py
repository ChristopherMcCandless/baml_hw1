import pika
import json
import time
import logging

try:

    logging.warning("warm up started")
    time.sleep(7)
    logging.warning("warm up finished")

    # Создаём подключение к серверу на локальном хосте
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    logging.warning("Получили конекшн к rabbitmq")
    
    # Объявляем очередь y_true
    channel.queue_declare(queue='y_true')
    logging.warning("Объявили очередь y_true")
    # Объявляем очередь y_pred
    channel.queue_declare(queue='y_pred')
    logging.warning("Объявили очередь y_pred")
    
    # Создаём функцию callback для обработки данных из очереди
    def callback(ch, method, properties, body):
        logging.warning(f'Из очереди {method.routing_key} получено значение {json.loads(body)}')
    
    # Извлекаем сообщение из очереди y_true
    channel.basic_consume(
        queue='y_true',
        on_message_callback=callback,
        auto_ack=True
    )
    # Извлекаем сообщение из очереди features
    channel.basic_consume(
        queue='y_pred',
        on_message_callback=callback,
        auto_ack=True
    )
    
    logging.warning('...Начинаем консьюминг')
    channel.start_consuming()

except Exception as ex:
    logging.error(msg="Не удалось подключиться к очереди", exc_info=ex)
    time.sleep(3)
    raise