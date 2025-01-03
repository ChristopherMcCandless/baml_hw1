import pika
import json
import time
import logging
import numpy as np
import pandas as pd
import sys
sys.path.append('/logs')

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
        try:
            logging.warning(f'Из очереди {method.routing_key} получено значение {json.loads(body)}')

            msg_data = json.loads(body)
            msg_id = msg_data['id']
            msg_value = float(msg_data['body'])

            file_path = "/logs/metric_log.csv"
            df = pd.read_csv(file_path, sep=',')

            row_index = -1
            if (df['id'] == msg_id).any():
                row_index = df.query(f'id=="{msg_id}"').index

            if method.routing_key == "y_true":
                if row_index == -1:
                    newRow = pd.Series({
                        'id': msg_id,
                        'y_true': msg_value,
                        'y_pred': None,
                        'absolute_error': None
                    })
                    df = pd.concat([df, pd.DataFrame([newRow], columns=newRow.index)]).reset_index(drop=True)
                else:
                    df.loc[row_index, 'y_true'] = msg_value


            if method.routing_key == "y_pred":
                if row_index == -1:
                    newRow = pd.Series({
                        'id': msg_id,
                        'y_true': None,
                        'y_pred': msg_value,
                        'absolute_error': None
                    })
                    df = pd.concat([df, pd.DataFrame([newRow], columns=newRow.index)]).reset_index(drop=True)
                else:
                    df.loc[row_index, 'y_pred'] = msg_value
                    
            if row_index != -1:
                y_pred = df.loc[row_index]['y_pred'].values[0]
                y_true = df.loc[row_index]['y_true'].values[0]

                if np.isnan(y_pred) == False and np.isnan(y_true) == False:
                    df.loc[row_index, 'absolute_error'] = float(abs(float(y_true) - float(y_pred)))

            df.to_csv(file_path, sep=',', index=False)
            logging.warning(f'Metric log обновлен')
        except Exception as e:
            logging.error("Ошибка при обработке сообщения", exc_info=e)
    
    # Извлекаем сообщение из очереди y_true
    channel.basic_consume(
        queue='y_true',
        on_message_callback=callback,
        auto_ack=True
    )
    # Извлекаем сообщение из очереди y_pred
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