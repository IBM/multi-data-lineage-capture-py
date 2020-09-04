from provlake.persistence.persister import Persister
from provlake.model.activity_prov_obj import ProvRequestObj
from provlake.utils.constants import StandardNamesAndIds
import json
import logging
logger = logging.getLogger('PROV')


class UnmanagedPersister(Persister):

    def __init__(self, workflow_name: str, wf_start_time: float, log_dir: str):
        super().__init__(workflow_name, wf_start_time)
        self.log_file_path = StandardNamesAndIds.get_prov_log_file_path(log_dir, workflow_name, wf_start_time)

    def add_request(self, persistence_request: ProvRequestObj):
        self._append_log(persistence_request.as_dict())

    def _append_log(self, retrospective_json: dict):
        try:
            with open(self.log_file_path, 'a') as f:
                f.writelines("{}\n".format(json.dumps([retrospective_json])))
        except Exception as e:
            logger.error("Could not save prov logs in " + self.log_file_path + "\n" + str(e))
            pass
