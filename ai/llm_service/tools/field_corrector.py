from typing import Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel


def levenshtein_distance(a: str, b: str) -> int:
    """Classic DP Levenshtein — O(m·n) time, O(n) space."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)

    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i] + [0] * len(b)
        for j, cb in enumerate(b, 1):
            curr[j] = min(
                prev[j] + 1,  # deletion
                curr[j - 1] + 1,  # insertion
                prev[j - 1] + (ca != cb),  # substitution
            )
        prev = curr
    return prev[-1]


def handle_literal(data: Any, model: Any):
    literal_values = get_args(model)

    # Exact match
    for val in literal_values:
        if data == val:
            return data

    # Handle nested models/classes inside Literal
    # e.g. Literal["x", SomeModel]
    for val in literal_values:
        if hasattr(val, "model_fields"):
            if isinstance(data, dict):
                res = correct_fields(data, val)
                if res not in (None, {}):
                    return res

    # String normalization
    if isinstance(data, str):
        normalized = data.strip().lower()

        for val in literal_values:
            if isinstance(val, str):
                if normalized == val.strip().lower():
                    return val

    # Conservative fuzzy matching
    if isinstance(data, str):
        matches = []

        for val in literal_values:
            if not isinstance(val, str):
                continue

            dist = levenshtein_distance(
                normalized,
                val.strip().lower(),
            )

            if dist <= 1:
                matches.append((dist, val))

        matches.sort(key=lambda x: x[0])

        # only allow unambiguous match
        if len(matches) == 1:
            return matches[0][1]

    print(f"Invalid Literal value: {data} not in {literal_values}")
    return {}


def correct_fields(data: Any, model: Any):
    if type(data) is list:
        list_item_types = get_args(model)
        if len(list_item_types) == 0:
            print(f"Empty Args type: {data} | {model}")
            return {}
        list_item_type = list_item_types[0]
        for i in range(len(data)):
            res = correct_fields(data[i], list_item_type)
            if res is None or res == {}:
                continue
            data[i] = res
        return data
    if type(data) is not dict:
        return None
    if get_origin(model) is dict:
        return None

    updates = []

    if "model_fields" not in dir(model):
        # Account for Optional
        if get_origin(model) is Union and type(None) in get_args(model):
            return correct_fields(
                data, [arg for arg in get_args(model) if arg is not type(None)][0]
            )
        # Account for Literal
        if get_origin(model) is Literal:
            return handle_literal(data, model)

        print(f"ERROR: model_fields attribute doesn't exist: \n{data}\n{model}")
    model_fields = model.model_fields
    for model_key in model_fields.keys():
        best_match = (9999, None)
        for key in data.keys():
            misspelling_dist = levenshtein_distance(key, model_key)
            if misspelling_dist < best_match[0]:
                best_match = (misspelling_dist, key)
            if misspelling_dist == 0.0:
                best_match = (misspelling_dist, key)
                break
        if best_match[0] / len(model_key) >= 0.5:
            print("Model Key very distinct from all fields. Not updating")
            continue

        # check for no updates
        if best_match[0] == 9999:
            continue
        assert best_match[1] is not None

        # if no error: continue
        if best_match[0] == 0.0:
            continue

        updates.append((model_key, best_match[1]))

    for update in updates:
        model_key, key = update

        # update key error
        data[model_key] = data[key]
        del data[key]

    for key in data.keys():
        if key not in model_fields:
            print(f"Key is not in model_fields: {key}")
            continue
        res = correct_fields(data[key], model_fields[key].annotation)
        if res is None or res == {}:
            continue
        data[key] = res

    return data


def try_correct_fields(data: dict[str, Any], model: BaseModel):
    res = correct_fields(data, model)
    if res is None or res == {}:
        return data
    return res
