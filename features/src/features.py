import pika
import numpy as np
import json
from sklearn.datasets import load_diabetes
import time
from datetime import datetime
import logging
import uuid
 
# Создаём бесконечный цикл для отправки сообщений в очередь
while True:
    try:

        logging.warning("warm up started")
        time.sleep(3)
        logging.warning("warm up finished")

        logging.warning("Начинаем загружать датасет")
        X, y = load_diabetes(return_X_y=True)
        logging.warning("Загрузили датасет")

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()
        logging.warning("Получили конекшн к rabbitmq")
        
        try:
            while True:
                # Формируем случайный индекс строки
                random_row = np.random.randint(0, X.shape[0]-1)
                message_id = str(uuid.uuid4())
                message_y_true_body = {
                    'id': message_id,
                    'body': y[random_row]
                }
                logging.info("Сформировали сообщение для y_true очереди")

                # Публикуем сообщение в очередь y_true
                channel.basic_publish(
                    exchange='',
                    routing_key='y_true',
                    body=json.dumps(message_y_true_body)
                )
                print('Сообщение с правильным ответом отправлено в очередь')
        
                # Публикуем сообщение в очередь features
                message_features_body = {
                    'id': message_id,
                    'body': list(X[random_row])
                }
                logging.info("Сформировали сообщение для features очереди")
                channel.basic_publish(
                    exchange='',
                    routing_key='features',
                    body=json.dumps(message_features_body)
                )
                print('Сообщение с вектором признаков отправлено в очередь')

                #добавляем делей
                logging.info("Delay starting")
                time.sleep(10)
                logging.info("Delay finished")
        except Exception as ex:
            print(f"publish message error {ex}")
    
        # Закрываем подключение
        logging.warning("Закрываем подключение к rabbitmq")
        connection.close()
        logging.warning("Закрыли подключение к rabbitmq")

    except Exception as ex:
        logging.error(msg="Не удалось подключиться к очереди", exc_info=ex)
        time.sleep(3)
        #raise