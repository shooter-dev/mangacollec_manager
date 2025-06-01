# manager_app/manager.py
import os
import time
from http import HTTPStatus
from typing import Dict, List

import requests
from celery import Celery
from celery.result import AsyncResult

import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine, Engine

from mangacollec_api.entity.serie import Serie
from mangacollec_api.client import MangaCollecAPIClient
from mangacollec_api.endpoints.serie_endpoint import SerieEndpoint


class Manager:
    list_proxy_index: int = 0

    list_proxy = [
        "51.195.137.60:80",
        "51.38.191.151:80",
        "51.91.109.83:80",
        "149.202.91.219:80",
        "217.182.210.152:80",
        "151.80.56.88:80",
        "94.23.9.170:80",
        "178.170.9.226:80",
        "158.255.77.166:80",
        "158.255.77.168:80",
        "158.255.77.169:80",
        "62.210.215.36:80",
        "62.210.15.199:80",
        "84.103.174.6:80",
        "147.135.128.218:80",
        "145.239.196.123:80",
        "54.38.181.125:80",
        "51.15.228.52:8080",
        "51.254.121.123:8080",
        "51.81.187.228:3128",
        "151.80.199.88:3128",
        "51.222.158.98:3128",
        "54.37.207.54:3128",
        "51.222.96.27:9595",
        "90.52.80.102:8888",
        "40.66.47.250:3128",
        "62.210.222.58:3128",
        "51.159.110.214:3128",
        "163.172.54.157:3128",
    ]

    is_finish: bool = False

    _list_status_tasks: List[str] = []
    _tasks_en_cour: List[AsyncResult] = []

    def __init__(self):

        engine_text: str = (
            f"postgresql+psycopg2://{os.environ.get('POSTGRES_USER')}:"
            f"{os.environ.get('POSTGRES_PASSWORD')}@"
            f"{os.environ.get('POSTGRES_HOST')}:"
            f"{os.environ.get('POSTGRES_PORT')}/"
            f"{os.environ.get('POSTGRES_DB')}"
        )

        self.client_mangacollec = MangaCollecAPIClient(
            os.environ.get("CLIENT_ID"),
            os.environ.get("CLIENT_SECRET")
        )

        self.engine: Engine = create_engine(engine_text)

        self.__broker: str = (
            f"pyamqp://{os.environ.get('RABBITMQ_DEFAULT_USER')}:{os.environ.get('RABBITMQ_DEFAULT_PASS')}@{os.environ.get('RABBITMQ_DEFAULT_URL')}//"
        )
        self.__backen: str = "db+sqlite:///results.db"

        # On crée uniquement un client Celery pointant vers le même broker
        self.client = Celery("url_app", broker=self.__broker, backend=self.__backen)

    def __get_list_series_from_api(self) -> List[Serie]:

        serie_endpoint = SerieEndpoint(self.client_mangacollec)

        return serie_endpoint.get_all_series_v2()

    def __envoi_url_fetch_queue(self, url: str) -> AsyncResult:
        # print(f"[PROCESS] Envoi l'url {url} ... ")

        # selection un proxy dans la list des proxies
        proxy = self.list_proxy[self.__iter_list_proxy()]

        result: AsyncResult = self.client.send_task("call_serie_api", args=[url, proxy], queue="call_serie_api")

        return result

    def __iter_list_proxy(self) -> int:

        if self.list_proxy_index < len(self.list_proxy) - 1:
            self.list_proxy_index += 1

        else:
            self.list_proxy_index = 0

        return self.list_proxy_index

    def start(self):
        series: List[Serie] = self.__get_list_series_from_api()

        self._tasks_en_cour = []
        c: int = 1

        t: int = len(series)

        for serie in series[:3]:
            self._tasks_en_cour.append(self.__envoi_url_fetch_queue(serie.id))

            print(f"[PROCESS][ID SERIE][STATUS] envoi {c} / {t}")

            # Rajoute du temps pour alléger la bande passante
            time.sleep(0.02)
            c += 1

        print("[PROCESS] Envoi Des Url Terminer")

        self.__proccess_tasks_result()

    def __proccess_tasks_result(self):
        debut = time.time()
        success_count: int = 0
        while not self.is_finish:
            print("######################################")

            self._list_status_tasks.clear()

            for task in self._tasks_en_cour:
                value = task.state

                self._list_status_tasks.append(value)

                if value == "SUCCESS":
                    self._tasks_en_cour.remove(task)
                    success_count += 1

                # Rajoute du temps pour alléger la bande passante
                time.sleep(0.02)

            # Affichage des différents états des tâches

            print(f"[PENDING]: {self._list_status_tasks.count('PENDING')}")
            print(f"[STARTED]: {self._list_status_tasks.count('STARTED')}")
            print(f"[RETRY]  : {self._list_status_tasks.count('RETRY')}")
            print(f"[SUCCESS]: {self._list_status_tasks.count('SUCCESS') + success_count}")
            print(f"[PROCESS][INFO][STATUS] len tasks : {len(self._tasks_en_cour)} succes_count : {success_count}")

            self.__is_proccess_tasks_finish()

            time.sleep(5)

        end = time.time()
        duree_secondes = end - debut
        heures = int(duree_secondes // 3600)
        minutes = int((duree_secondes % 3600) // 60)
        secondes = int(duree_secondes % 60)
        print(f"[FINISH][INFO][STATUS] {heures} heures, {minutes} minutes et {secondes} secondes")

    def __is_proccess_tasks_finish(self):
        if self._list_status_tasks.count("SUCCESS") == len(self._list_status_tasks):
            self.is_finish = True

    def nettoyage_resulta(self):
        query: str = f"""SELECT *
                        FROM {os.environ.get('POSTGRES_TABLE')}
        """

        df: DataFrame = pd.read_sql(query, self.engine, "id")

        print("HEAD")
        print(df.head())
        print("")
        print("INFO")
        print(df.info)
        print("")
        print("TYPE")
        print(df.dtypes)
        print("")
        print("COUNT")
        print(df.count())
        print("################################")
        df_clean = df.drop_duplicates()
        print("CLEAN")
        print(df_clean.count())

        print()

        self.sauvegarde(df_clean)

    def result(self):
        self.nettoyage_resulta()

    def sauvegarde(self, df_clean: DataFrame):
        df_clean.to_sql(name="data_clean", con=self.engine, if_exists="replace", index_label="id")
        df_clean.to_sql(name="data_worker", con=self.engine, if_exists="replace", index_label="id")
        df_clean.to_csv("data.csv")


if __name__ == "__main__":
    manager = Manager()
    manager.start()
    # manager.result()
