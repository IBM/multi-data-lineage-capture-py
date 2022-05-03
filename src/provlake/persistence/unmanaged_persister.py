from provlake.persistence.persister import Persister
from provlake.model.activity_prov_obj import ProvRequestObj
from provlake.utils.constants import StandardNamesAndIds
import json
import logging
logger = logging.getLogger('PROV')


class UnmanagedPersister(Persister):

    def __init__(self, log_file_path: str):
        super().__init__(log_file_path)

    def add_request(self, persistence_request: ProvRequestObj):
        self._append_log(persistence_request.as_dict())

    def get_file_path(self):
        return self.log_file_path

    def _append_log(self, retrospective_json: dict):
        try:
            with open(self.log_file_path, 'a') as f:
                f.writelines("{}\n".format(json.dumps([retrospective_json])))
        except Exception as e:
            logger.error("Could not save prov logs in " + self.log_file_path + "\n" + str(e))
            pass

    def __del__(self):
        logging.warning('Calling the destructor')

