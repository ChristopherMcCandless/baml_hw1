import pika
import pickle
import numpy as np
import json
import logging
import time
 
# Читаем файл с сериализованной моделью
with open('myfile.pkl', 'rb') as pkl_file:
    regressor = pickle.load(pkl_file)
 
try:

    logging.warning("warm up started")
    time.sleep(5)
    logging.warning("warm up finished")

    # Создаём подключение по адресу rabbitmq:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    logging.warning("Получили конекшн к rabbitmq")
 
    # Объявляем очередь features
    channel.queue_declare(queue='features')
    logging.warning("Объявили очередь features")
    # Объявляем очередь y_pred
    channel.queue_declare(queue='y_pred')
    logging.warning("Объявили очередь y_pred")
 
    # Создаём функцию callback для обработки данных из очереди
    def callback(ch, method, properties, body):
        logging.warning(f'Получен вектор признаков {body}')
        msg_data = json.loads(body)
        msg_id = msg_data['id']
        features = msg_data['body']

        pred = regressor.predict(np.array(features).reshape(1, -1))
        y_pred_msg = {
            'id': msg_id,
            'body': pred[0]
        }
        channel.basic_publish(
            exchange='',
            routing_key='y_pred',
            body=json.dumps(y_pred_msg)
        )
        logging.warning(f'Предсказание {pred[0]} отправлено в очередь y_pred')
 
    # Извлекаем сообщение из очереди features
    # on_message_callback показывает, какую функцию вызвать при получении сообщения
    channel.basic_consume(
        queue='features',
        on_message_callback=callback,
        auto_ack=True
    )

    logging.warning('...Ожидание сообщений')
    channel.start_consuming()

    # Закрываем подключение
    logging.warning("Закрываем подключение к rabbitmq")
    connection.close()
    logging.warning("Закрыли подключение к rabbitmq")

except Exception as ex:
    logging.error(msg="Не удалось подключиться к очереди", exc_info=ex)
    time.sleep(3)
    raise