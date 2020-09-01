import traceback
from requests_futures.sessions import FuturesSession
from time import sleep
from urllib.parse import urljoin
import json
from requests.exceptions import ConnectionError
import urllib3
import logging
from typing import List
logger = logging.getLogger('PROV')
offline_prov_log = logging.getLogger("OFFLINE_PROV")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class _ProvPersister:

    def __init__(self, df_name: str, service_url: str = "http://localhost:5000", context: str = None,
                 with_validation: bool = False, db_name: str = None,
                 bag_size: int = 1, should_store_offline_log: bool = False, online: bool = True):
        self.retrospective_url = urljoin(service_url, "retrospective-provenance")
        self.prospective_url = urljoin(service_url, "prospective-provenance")
        self.df_name = df_name
        self.context = context
        self.with_validation = with_validation
        self.db_name = db_name
        self.session = FuturesSession()
        self.requests_queue = list()
        self.bag_size = bag_size
        self.online = online
        self.should_store_offline_log = should_store_offline_log

        if self.online:
            logger.debug("You are using the Service URL: " + service_url)

    def close(self, wf_prov_obj: dict):
        while not self.session.executor._work_queue.empty():
            # wait to guarantee that all provenance requests have been sent to collector service
            sleep(0.1)
        self.persist_workflow(wf_prov_obj)
        # Persist remaining tasks synchronously
        self.__flush__(all_and_wait=True)
        self.session.close()

    def persist_task(self, prov_obj: dict):
        try:
            data = {
                "prov_obj": prov_obj,
                "dataflow_name": self.df_name,
                "act_type": "task"
            }
            if self.context:
                data["context"] = self.context
            self.requests_queue.append(data)
            if len(self.requests_queue) >= self.bag_size:
                self.__flush__()
        except Exception:
            logger.error("[Prov] Unexpected exception")
            traceback.print_exc()
            pass

    def persist_workflow(self, prov_obj: dict):
        try:
            data = {
                "prov_obj": prov_obj,
                "dataflow_name": self.df_name,
                "act_type": "workflow"
            }
            if self.context:
                data["context"] = self.context
            self.requests_queue.append(data)
            # if `configuration` is present this object should be persisted synchronously
            if "configuration" in prov_obj:
                self.__flush__(True)
        except Exception:
            logger.error("[Prov] Unexpected exception")
            traceback.print_exc()
            pass

    def __flush__(self, all_and_wait: bool = False):
            if len(self.requests_queue) > 0:
                if all_and_wait:
                    logger.debug("Going to flush everything. Flushing " + str(len(self.requests_queue)))
                    if self.should_store_offline_log:
                        offline_prov_log.debug(json.dumps(self.requests_queue))
                    if self.online:
                        self.__persist_online__(self.requests_queue)
                    self.requests_queue = list()
                else:
                    to_flush = self.requests_queue[:self.bag_size]
                    del self.requests_queue[:self.bag_size]
                    logger.debug("Going to flush a part. Flushing " + str(len(to_flush)) + " out of " +
                                str(len(self.requests_queue)))
                    if self.should_store_offline_log:
                        offline_prov_log.debug(json.dumps(to_flush))
                    if self.online:
                        self.__persist_online__(to_flush)

    def __persist_online__(self, to_flush: List[dict]):
        params = {"with_validation": str(self.with_validation), "db_name": self.db_name}
        try:
            logger.debug("[Prov-Persistence]" + json.dumps(to_flush))
            r = self.session.post(self.retrospective_url, json=to_flush, params=params, verify=False).result()
        except ConnectionError as ex:
            logger.error(
                "[Prov][ConnectionError] There is a communication error between client and server -> " + str(ex))
            r = None
            pass
        except Exception as ex:
            traceback.print_exc()
            logger.error(
                "[Prov] Unexpected exception while adding retrospective provenance: " + type(ex).__name__
                + "->" + str(ex))
            r = None
            pass
        # If requests were validated, check for errors
        if r and self.with_validation:
            self.__log_validation_msg__(r)

    def persist_prospective(self, json_data: dict):
        try:
            if self.should_store_offline_log:
                offline_prov_log.debug(json.dumps(self.requests_queue))
            if self.online:
                logger.debug("[Prov-Persistence][Prospective]" + json.dumps(json_data))
                try:
                    r = self.session.post(self.prospective_url, json=json_data, params={'overwrite': True},
                                          verify=False).result()
                    if 200 <= r.status_code <= 209:
                        logger.debug("Prospective provenance inserted successfully.")
                    elif r.status_code == 406:
                        error_parsed = json.loads(r._content.decode('utf-8'))
                        error_obj = error_parsed['error'].replace("'", '"')
                        logger.error(error_obj)
                    elif r.status_code == 500:
                        r = self.session.put(self.prospective_url, json=json_data).result()
                        try:
                            assert 200 <= r.status_code <= 209
                        except AssertionError:
                            logger.error("Prospective provenance was not inserted correctly. Status code = " + str(r.status_code))
                    elif r.status_code > 300:
                        logger.error("Prospective provenance was not inserted correctly. Status code = " + str(r.status_code))
                except ConnectionError as ex:
                    traceback.print_exc()
                    logger.error("[Prov][ConnectionError] There is a communication error between client and server -> " + str(
                            ex))
                    pass
                except Exception as ex:
                    logger.error("[Prov] Unexpected exception while adding prospective provenance: " + type(ex).__name__)
                    pass
        except Exception as ex:
            logger.error("[Prov] Unexpected exception " + type(ex).__name__)
            traceback.print_exc()
            pass

    @staticmethod
    def __log_validation_msg__(response):
        error_obj = json.loads(response._content.decode('utf-8'))
        if len(error_obj['error']) > 0:
            for error_list in error_obj['error']:
                for error in error_list:
                    if error['code'][0] == 'W':
                        logger.warning('{} {}{}'.format(error['type'], error['explanation'], '\n'))
                    else:
                        logger.error('{} {}{}'.format(error['type'], error['explanation'], '\n'))

