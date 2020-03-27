import numpy as np
import traceback
import logging
logger = logging.getLogger('PROV')


class ProvTask:

    def __init__(self, prov, data_transformation_name: str, input_args, context_is_managed=True):
        """
        :param prov:
        :param data_transformation_name:
        :param input_args:
        :param context_is_managed: True is the default, meaning that the context (Python's Context Manager) is being
                                   managed. Set to False if you want to use ProvTask without Context Management.
        """
        self.prov = prov
        self.task_id = None
        self.data_transformation_name = data_transformation_name
        self.input_args = input_args
        self.stored_output = False
        self.context_is_managed = context_is_managed
        if not self.context_is_managed:
            self.__enter__()

    def __convert_args(self, args, dataset_type: str="i"):
        if type(args) == dict:
            return args
        else:
            assert self.prov.get_dataflow_structure(), "If you are not informing the prospective provenance, you must " \
                                                       "use dictionaries to be captured. Check your input or output arguments."

            if dataset_type == "i":
                ds = "input_datasets"
            elif dataset_type == "o":
                ds = "output_datasets"

            ds_arr = self.prov.get_dataflow_structure()["data_transformations"][self.data_transformation_name][ds]
            if len(ds_arr) > 0:
                # TODO: currently we are assuming all data transformations manipulate 1 dataset in prosp. provenance
                # Following line needs to be changed if we need more datasets.
                attributes = ds_arr[0]["attributes"]
                ret = dict()
                if type(args) == list:
                    i = 0
                    for val in args:
                        ret[attributes[i]["name"]] = val
                        i += 1
                    return ret
                elif np.isscalar(args):
                    if len(attributes) > 0:
                        ret[attributes[0]["name"]] = args
                        return ret
                    else:
                        logger.error("[Prov] This is likely an error.")
                        return None
            else:
                return {}

    def __enter__(self):
        try:
            if self.prov:
                self.task_id = self.prov.collect_in(self.data_transformation_name,
                                                    values=self.__convert_args(self.input_args))
        except Exception as e:
            logger.error(str(e))
            traceback.print_exc()
            logger.error("Error storing provenance for " +
                         self.data_transformation_name + " args: " + str(self.input_args))
            pass
        return self

    def __exit__(self, *args):
        try:
            if self.prov and not self.stored_output:
                # There is no output, but end of task should be recorded anyway.
                self.prov.collect_out(self.task_id, self.data_transformation_name, {}, "", "")
        except Exception as e:
            logger.error(str(e))
            traceback.print_exc()
            pass

    def extend_input(self, args: dict):
        try:
            if self.prov:
                self.__extend(args, "Input")
        except Exception as e:
            traceback.print_exc()
            logger.error(str(e))
            logger.error("Error storing ext provenance for " +
                         self.data_transformation_name + " args: " + str(args))
            pass

    def extend_output(self, args: dict):
        try:
            self.__extend(args, "Output")
        except Exception as e:
            traceback.print_exc()
            logger.error(str(e))
            logger.error("Error storing ext provenance for " +
                         self.data_transformation_name + " args: " + str(args))
            pass

    def __extend(self, args: dict, dataset_type="Input"):
        self.prov.extend(self.task_id, self.data_transformation_name, values=args, dataset_type=dataset_type)

    def output(self, output_args=None, stdout=None, stderr=None):
        try:
            if output_args:
                if self.prov:
                    self.stored_output = True
                    self.prov.collect_out(self.task_id, self.data_transformation_name,
                                          values=self.__convert_args(output_args, "o"), stdout=stdout, stderr=stderr)
        except Exception as e:
            traceback.print_exc()
            logger.error(str(e))
            logger.error("Error storing out provenance for " + self.data_transformation_name + " args: " + str(output_args))
            pass

        if not self.context_is_managed:
            self.__exit__()
