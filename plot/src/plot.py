import time
import logging
import pandas as pd
import seaborn as sns

while True:
    try:
        logging.warning("warm up started")
        time.sleep(10)
        logging.warning("warm up finished")

        file_path = "/logs/metric_log.csv"
        df = pd.read_csv(file_path, sep=',')
        df = df.dropna()

        plot = sns.histplot(df, x='absolute_error')
        fig = plot.get_figure()
        fig.savefig("/logs/error_distribution.png")

        logging.warning("hist plot was saved")
        time.sleep(30)

    except Exception as ex:
        logging.error("Ошибка при обработке лога метрик", exc_info=ex)