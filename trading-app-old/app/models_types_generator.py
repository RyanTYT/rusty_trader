import ast
import re
from typing import TypedDict, List, Dict, Tuple
from app.utils.custom_logging import CustomLogger

# """
# Example Usage of this file:
#     Given a file 'models.py' with the following content:
#         class ExampleModel(db.Model):
#             id: int = db.Column(db.Integer, primary_key=True)
#             name: str = db.Column(db.String(50))
#
#     The function returns:
#         [
#             {
#                 'name': 'ExampleModel',
#                 'fields': {
#                     'id': 'int',
#                     'name': 'str'
#                 }
#             }
#         ]
#
# Limitations:
#     - Only detects classes using explicit `db.Column` declarations for their fields.
#     - Field types that cannot be resolved to a known SQLAlchemy type will default to 'Unknown'.
# """

SQLALCHEMY_TYPE_MAP = {
    "String": "str",
    "Integer": "int",
    "Float": "float",
    "DateTime": "datetime.datetime",
    "TIMESTAMP": "datetime.datetime",
    "Boolean": "bool",
    "Text": "str",
    "Numeric": "float",
    "BigInteger": "int",
    "SmallInteger": "int",
    "Time": "datetime.time",
    "Date": "datetime.date",
    "JSON": "dict",
    "UUID": "str",
}


class ExtractedModels(TypedDict):
    name: str
    db_name: str
    primary_keys: Dict[str, str]
    fields: Dict[str, str]
    required_keys: List[str]


class ExtractedEnums(TypedDict):
    name: str
    bases: List[str]
    fields: Dict[str, str]


custom_logger = CustomLogger(name="models_types_generator")


def extract_enum_types(file_path: str) -> List[ExtractedEnums]:
    with open(file_path, "r") as f:
        tree = ast.parse(f.read())

    models: List[ExtractedEnums] = []
    for node in ast.walk(tree):
        # Once Class is found
        if isinstance(node, ast.ClassDef):
            if len(node.bases) == 0 or not isinstance(node.bases[0], ast.Name):
                continue

            if (
                node.bases[0].id == "str"
                and isinstance(node.bases[1], ast.Name)
                and node.bases[1].id == "Enum"
            ):
                fields = {}
                for assignment in node.body:
                    if not isinstance(assignment, ast.Assign):
                        continue
                    if not isinstance(assignment.targets[0], ast.Name):
                        continue
                    if not isinstance(assignment.value, ast.Constant):
                        continue
                    fields[assignment.targets[0].id] = assignment.value.value
                models.append(
                    {
                        "name": node.name,
                        "bases": [node.bases[0].id, node.bases[1].id],
                        "fields": fields,
                    }
                )

    return models


def extract_model_classes(file_path: str) -> List[ExtractedModels]:
    """
    Parses a SQLAlchemy models.py file to extract model class names and their fields,
    as well as return a secondary dictionary containing only the fields which are primary keys

    Args:
        file_path (str): Path to the models.py file.

    Returns:
        list[dict]: A list of dictionaries, each containing the class name and its fields,
                    with fields mapped to their Python types (e.g., {"name": "CurrentPosition", "fields": {"stock": "str"}}).
        list[dict]: A list of dictionaries, each containing the class name and its fields which are primary keys,
                    with fields mapped to their Python types (e.g., {"name": "CurrentPosition", "fields": {"stock": "str"}}).
    """
    with open(file_path, "r") as f:
        tree = ast.parse(f.read())

    models: List[ExtractedModels] = []
    for node in ast.walk(tree):
        # Once Class is found
        if isinstance(node, ast.ClassDef):
            if (
                len(node.bases) == 0
                or not isinstance(node.bases[0], ast.Name)
                or node.bases[0].id != "Base"
            ):
                continue

            # custom_logger.info(f"Found class: {node.name}")  # Debug: Show class name
            model_class: ExtractedModels = {
                "name": node.name,
                "fields": {},
                "primary_keys": {},
                "required_keys": [],
                "db_name": "",
            }

            # Traverse all statements in the class body
            for item in node.body:
                if isinstance(item, ast.Assign):
                    if (
                        isinstance(item.targets[0], ast.Name)
                        and item.targets[0].id == "__tablename__"
                        and isinstance(item.value, ast.Constant)
                    ):
                        model_class["db_name"] = item.value.value
                        continue
                    if (
                        isinstance(item.targets[0], ast.Name)
                        and item.targets[0].id == "__table_args__"
                    ):
                        if isinstance(item.value, ast.Tuple):
                            for tuple_item in item.value.elts:
                                if isinstance(tuple_item, ast.Dict) and isinstance(
                                    tuple_item.values[0], ast.Constant
                                ):
                                    model_class["db_name"] = (
                                        tuple_item.values[0].value
                                        + "."
                                        + model_class["db_name"]
                                    )
                                    break
                        elif isinstance(item.value, ast.Dict) and isinstance(
                            item.value.values[0], ast.Constant
                        ):
                            model_class["db_name"] = (
                                item.value.values[0].value
                                + "."
                                + model_class["db_name"]
                            )
                        continue
                    else:
                        custom_logger.error("Unknown assignmet encountered!!")

                # For annotated assignments (e.g., field: str = ...)
                if isinstance(item, ast.AnnAssign) and isinstance(
                    item.target, ast.Name
                ):
                    target: ast.Name = item.target
                    field_name = target.id
                    # custom_logger.info(
                    #     f"  Processing field: {field_name}"
                    # )  # Debug: Show field being processed
                    if (
                        isinstance(item.value, ast.Call)
                        and isinstance(item.value.func, ast.Name)
                        and item.value.func.id == "mapped_column"
                    ):
                        # Extract field type
                        field_type, is_primary_key, is_required = extract_field_type(
                            item.value, models
                        )
                        if is_primary_key:
                            model_class["primary_keys"][field_name] = field_type
                        if is_required:
                            model_class["required_keys"].append(field_name)
                        model_class["fields"][field_name] = field_type
                        # custom_logger.info(
                        #     f"    Field type: {field_type}"
                        # )  # Debug: Show field type

            # if model_class["fields"]:
            #     custom_logger.info(
            #         f"  Fields: {model_class['fields']}"
            #     )  # Debug: Show fields for the model
            # else:
            #     custom_logger.error("  No fields found.")  # Debug: No fields found
            models.append(model_class)
    return models


def extract_field_type(
    column_call: ast.Call, models: List[ExtractedModels]
) -> Tuple[str, bool, bool]:
    """
    Extract field type and check if the field is part of the primary key from a db.Column() call.

    This function analyzes a `db.Column()` call in SQLAlchemy and extracts the field type
    (e.g., `str`, `int`, `float`) while also checking if the `primary_key` argument is set to `True`.

    Args:
        column_call (ast.Call): The AST node representing the `db.Column()` call.

    Returns:
        tuple: A tuple containing:
            - field_type (str): The SQLAlchemy field type as a string (e.g., 'str', 'int').
            - primary_key (bool): A boolean indicating whether the field is part of the primary key (`True` if it is, `False` otherwise).
            - is_required (bool): A boolean indicating whether the field is required.
    """
    # Check if there are arguments in db.Column(...)
    if column_call.args:
        first_arg = column_call.args[0]
        # Handles cases like db.String
        if isinstance(first_arg, ast.Attribute):
            sqlalchemy_type = first_arg.attr
            field_type = SQLALCHEMY_TYPE_MAP.get(sqlalchemy_type, "Unknown")

        # Handles cases like Integer, Float
        elif isinstance(first_arg, ast.Name):
            sqlalchemy_type = first_arg.id
            field_type = SQLALCHEMY_TYPE_MAP.get(sqlalchemy_type, "Unknown")

        # Handles cases like db.String(50), db.Float(precision=2)
        elif isinstance(first_arg, ast.Call) and isinstance(first_arg.func, ast.Name):
            if first_arg.func.id == "ForeignKey":
                field_type = "Unknown"
                if isinstance(first_arg.args[0], ast.Constant):
                    db_table = first_arg.args[0].value
                    attribute_search_res = re.search(r"\.([^\.]*)$", db_table)
                    attribute = (
                        ""
                        if attribute_search_res is None
                        else attribute_search_res.group(1)
                    )
                    for model in models:
                        if (
                            model["db_name"][: len(db_table) - len(attribute) - 1]
                            == db_table[: len(db_table) - len(attribute) - 1]
                        ):
                            field_type = model["fields"][attribute]
                            break
            elif first_arg.func.id == "PgEnum" and isinstance(
                first_arg.args[0], ast.Name
            ):
                field_type = first_arg.args[0].id
            else:
                sqlalchemy_type = first_arg.func.id
                field_type = SQLALCHEMY_TYPE_MAP.get(sqlalchemy_type, "Unknown")

        # Check if primary_key=True is set
        primary_key = False
        for keyword in column_call.keywords:
            if (
                keyword.arg == "primary_key"
                and isinstance(keyword.value, ast.Constant)
                and keyword.value.value is True
            ):
                primary_key = True

        # Check if primary_key=True is set
        is_required = primary_key
        for keyword in column_call.keywords:
            if (
                keyword.arg == "required"
                and isinstance(keyword.value, ast.Constant)
                and keyword.value.value is True
            ):
                is_required = True

        return field_type, primary_key, is_required

    return (
        "Unknown",
        False,
        False,
    )  # Return Unknown if no match is found, and False for primary_key


def generate_enums(models: List[ExtractedEnums]) -> str:
    result = []
    for model in models:
        class_definition = f"class {model['name']}({', '.join(model['bases'])}):\n"
        fields = "\n".join([f'    {k} = "{v}"' for k, v in model["fields"].items()])
        result.append(class_definition + fields)

    return "\n\n\n".join(result)


def generate_typeddict(models: List[ExtractedModels]) -> Tuple[str, bool]:
    """
    Generate TypedDict class definitions from extracted SQLAlchemy models.

    Args:
        models (List[dict]): A list of model dictionaries with:
            - "name" (str): Model name.
            - "fields" (dict): Field names mapped to Python types.
            - "primary_keys" (dict): Field names mapped to Python types containing only the primary key fields

    Returns:
        str: String containing TypedDict class definitions, including imports if datetime types are present.
             Classes with no fields will use `pass`.
        bool: True if datetime import is needed else False
    """
    contains_datetime = False  # Flag to determine if import from datetime is necessary
    result = []
    for model in models:
        typed_dict_name = model["name"] + "Dict"
        fields = model["fields"]
        primary_keys = model["primary_keys"]
        # required_keys = model["required_keys"]
        contains_datetime = contains_datetime or any(
            [
                value == "datetime.datetime"
                or value == "datetime.time"
                or value == "datetime.date"
                for value in fields.values()
            ]
        )

        # Build Class for Primary Keys
        primary_key_str = "\n".join(
            [f"    {key}: {value}" for key, value in primary_keys.items()]
        )
        primary_keys_class = f"class {typed_dict_name}PrimaryKeys(TypedDict):\n"
        primary_keys_class += primary_key_str if primary_keys else "    pass"
        primary_keys_class += "\n"

        # Build Class for Model - Inherits from Primary Keys
        field_str = "\n".join(
            [
                f"    {key}: {value}"
                for key, value in fields.items()
                if key not in primary_keys
                # f'    {key}: {value}' if key in required_keys else f'    {key}: Optional[{value}]' for key, value in fields.items() if key not in primary_keys
            ]
        )
        field_class = f"class {typed_dict_name}({typed_dict_name}PrimaryKeys):\n"
        field_class += field_str if fields else "    pass"
        field_class += "\n"

        # Build Class for Model - Inherits from Primary Keys
        update_str = "\n".join(
            [
                f"    {key}: NotRequired[{value}]"
                for key, value in fields.items()
                if key not in primary_keys
            ]
        )
        update_class = (
            f"class {typed_dict_name}UpdateKeys({typed_dict_name}PrimaryKeys):\n"
        )
        update_class += update_str if fields else "    pass"
        update_class += "\n"

        result.append(primary_keys_class)
        result.append(field_class)
        result.append(update_class)
    return "\n\n".join(result), contains_datetime


def generate_models_types(file_path: str) -> None:
    # file_path = "app/models.py"  # Path to your models.py file
    models = extract_model_classes(file_path)
    enum_models = extract_enum_types(file_path)
    typed_dicts, contains_datetime = generate_typeddict(models)
    enums = generate_enums(enum_models)
    print(enums)

    with open("app/models_types.py", "w") as f:
        f.write("from typing import TypedDict\n")
        f.write("from typing_extensions import NotRequired\n")
        f.write("from enum import Enum")
        f.write("\nimport datetime\n\n\n" if contains_datetime else "\n\n\n")
        f.write(enums)
        f.write("\n\n\n")
        f.write(typed_dicts)


if __name__ == "__main__":
    generate_models_types("app/models.py")
