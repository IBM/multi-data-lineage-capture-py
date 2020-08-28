import tests.offline_simple_synthetic_workflow_tests as workflow_script
import prospective_generator_test as wf_script2
import seismic_importer as wf_script
import dis
import inspect
import ast
import json
import traceback
from enum import Enum

class UnknownValue(Enum):
    FUNCTION_CALL = 1

dtypes = {
    int: "integer",
    float: "float",
    ast.List: "list",
    list: "list",
    ast.Str: "string",
    str: "string",
    ast.Dict: "complex_attribute",
    dict: "complex_attribute",
    bool: "boolean",
    UnknownValue.FUNCTION_CALL: ""
}


def add_default_attributes(data_transformations):
    prospective_json = dict()
    prospective_json["context_name"] = ""
    prospective_json["dataflow_name"] = ""
    prospective_json["description"] = ""
    prospective_json["storages"] = {}
    prospective_json["data_transformations"] = data_transformations
    return prospective_json

def get_value(ast_value, simple_attributes_dict=None):
    if isinstance(ast_value, ast.Num):
        return ast_value.n
    elif isinstance(ast_value, ast.Str):
        return ast_value.s
    elif isinstance(ast_value, ast.List):
        return [get_value(v) for v in ast_value.elts]
    elif isinstance(ast_value, ast.Name):
        return simple_attributes_dict[ast_value.id]
    elif isinstance(ast_value, ast.Call):
        return UnknownValue.FUNCTION_CALL
    elif isinstance(ast_value, ast.NameConstant):
        return ast_value.value
    else:
        return ast_value

def add_semantics(attributes: list, attr_type):
    for attr in attributes:
        if "in" in attr_type:
            attr["semantics"] = "PARAMETER"
        else:
            attr["semantics"] = "OUTPUT_VALUE"

        if attr["dtype"] == "list" and attr["elementdtype"] == "complex_attribute":
            add_semantics(attr["attributes"], attr_type)

def check_list_attribute(simple_attributes_dict, prospective_chunk, ast_attribute_value):
    if isinstance(ast_attribute_value, ast.List) and hasattr(ast_attribute_value, 'elts'):
        attribute = ast_attribute_value.elts
    else:
        attribute = ast_attribute_value

    if isinstance(attribute, list) and len(attribute) > 0:
        try:
            element_value = get_value(attribute[0])
            elementdtype = dtypes[type(element_value)]
            prospective_chunk["elementdtype"] = elementdtype
            if elementdtype == "complex_attribute":
                prospective_chunk["attributes"] = get_dataset_attributes(simple_attributes_dict, attribute[0])
        except AttributeError:
            traceback.print_exc()
    return prospective_chunk


def create_attribute(simple_attributes_dict, key: str, value):
    attribute = dict()
    attribute["name"] = key

    v = get_value(value, simple_attributes_dict)
    if isinstance(value, ast.Call) or isinstance(v, type(UnknownValue.FUNCTION_CALL)):
        attribute["dtype"] = dtypes[v]
    else:
        attribute["dtype"] = dtypes[type(v)]

    attribute = check_list_attribute(simple_attributes_dict, attribute, value)
    # attribute["description"] = ""
    return attribute

def get_dataset_attributes(simple_attributes_dict, body_value, body_target=None):
    attributes = list()
    if isinstance(body_value, ast.Dict):
        for k, v in zip(body_value.keys, body_value.values):
            attributes.append(create_attribute(simple_attributes_dict, k.s, v))

    elif isinstance(body_target, ast.Subscript) and isinstance(body_value, ast.Name):
        key = body_target.slice.value
        value = simple_attributes_dict[body_value.id]
        attributes.append(create_attribute(simple_attributes_dict, key.s, value))

    elif not isinstance(body_value, ast.Call):
        key = body_target.slice.value
        attributes.append(create_attribute(simple_attributes_dict, key.s, body_value))
    return attributes


def update_attributes_dict(complex_attr_by_variable, variable_name, simple_attributes_dict, value, target):
    if not complex_attr_by_variable.__contains__(variable_name):
        complex_attr_by_variable[variable_name] = list()
    attributes = get_dataset_attributes(simple_attributes_dict, value, target)
    complex_attr_by_variable[variable_name].extend(attributes)
    return complex_attr_by_variable


def check_ast_instance(ast_body_instance, complex_attr_by_variable, simple_attributes_dict):
    if isinstance(ast_body_instance, ast.Assign) and hasattr(ast_body_instance, "value"):
        for target in ast_body_instance.targets:
            value = ast_body_instance.value

            if isinstance(target, ast.Subscript):
                variable_name = target.value.id
                complex_attr_by_variable = update_attributes_dict(complex_attr_by_variable, variable_name,
                                                                  simple_attributes_dict, value, target)
            elif isinstance(target, ast.Name):
                variable_name = target.id
                if isinstance(value, ast.Dict):
                    complex_attr_by_variable = update_attributes_dict(complex_attr_by_variable, variable_name,
                                                                      simple_attributes_dict, value, target)
                else:
                    simple_attributes_dict[variable_name] = get_value(value)


def create_dataset(complex_attr_by_variable, used_args_variable_name, dt_name, data_transformations_json, args_type):
    dataset_type = "input_datasets" if "input" in args_type.lower() else "output_datasets"
    if not data_transformations_json.__contains__(dt_name):
        data_transformations_json[dt_name] = dict()
        data_transformations_json[dt_name]["input_datasets"] = []
        data_transformations_json[dt_name]["output_datasets"] = []

    if len(complex_attr_by_variable[used_args_variable_name]) > 0 and dt_name != None:
        add_semantics(complex_attr_by_variable[used_args_variable_name], args_type)
        attributes_json = dict()
        attributes_json["attributes"] = complex_attr_by_variable[used_args_variable_name]
        data_transformations_json[dt_name][dataset_type] = [attributes_json]


def create_prospective_json(function_name, file_name: str= "prospective.json"):
    workflow_src = inspect.getsource(function_name)
    workflow_ast = ast.parse(workflow_src)
    data_transformations_json = dict()
    data_transformation_name = ""

    for c1, wk_outer_body in enumerate(workflow_ast.body):
        values_by_variable_name = dict()
        simple_attributes = dict()
        for c2, wk_inner_body in enumerate(wk_outer_body.body):
            check_ast_instance(wk_inner_body, values_by_variable_name, simple_attributes)

            # Inspect function call "With ProvTask(...)"
            if isinstance(wk_inner_body, ast.With):
                for input_item in wk_inner_body.items:
                    if len(input_item.context_expr.args) == 3:
                        if input_item.context_expr.func.id == 'ProvTask':
                            data_transformation_name = input_item.context_expr.args[1].s
                            input_args_variable_name = input_item.context_expr.args[2].id
                            create_dataset(values_by_variable_name, input_args_variable_name,
                                           data_transformation_name, data_transformations_json, "input")

                for wk_inner_with_body in wk_inner_body.body:
                    check_ast_instance(wk_inner_with_body, values_by_variable_name, simple_attributes)
                    if isinstance(wk_inner_with_body, ast.Expr) and isinstance(wk_inner_with_body.value, ast.Call):
                        if wk_inner_with_body.value.func.attr == "output":
                            output_args_variable_name = wk_inner_with_body.value.args[0].id
                            create_dataset(values_by_variable_name, output_args_variable_name,
                                           data_transformation_name, data_transformations_json, "output")

        if len(data_transformations_json) > 0:
            prospective_json = add_default_attributes(data_transformations_json)

            print("\n\t\tJSON: {}\n\t\t\t".format(type(prospective_json)),prospective_json)
            print(prospective_json)
            with open(file_name, "w") as f:
                json.dump(prospective_json, f, indent=4)

if __name__ == '__main__':
    create_prospective_json(wf_script2.run_workflow, "prospective.json")
