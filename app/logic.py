from datetime import datetime
import json
import pm4py
import ast
from pydantic import BaseModel
import operator
from functools import reduce
import numpy as np
import pandas as pd
from fastapi.encoders import jsonable_encoder

log = {}
events: int = 0
columns: list = []


class Mapping(BaseModel):
    function: str
    contract: str
    block: str
    sender: str
    timestamp: str
    gasLimit: str
    gasUsed: str
    value: str
    SV: str
    CALL: str
    I: str
    E: str

'''
def loadLog(log: bytes):
    global data
    data = json.loads(log.decode('utf-8'))
    return len(data)
'''

def uploadLog(file: str):
    global log, events, columns
    print(f"file = {file}")
    print("COLONNE TROVATE NEL LOG:", columns)

    # reset variabili globali
    log = {}
    events = 0
    columns = []

    try:
        data = pm4py.read_xes(file)
        events = len(data)
        # print(data.columns)
        columns = data.columns.tolist()
        grouped = {}
        if "CryptoKitties" in file:
            grouped = data.groupby('case:ident:piid').apply(lambda x: x.to_dict(orient='records')).to_dict()
        else:
            grouped = data.groupby('case:concept:name').apply(lambda x: x.to_dict(orient='records')).to_dict()
        # Build data structure for the event log { 'case_id': [{event1},{event2},{event...},{eventN}]}
        log = {str(key): value for key, value in grouped.items()}
        
        print(f"log cases: {len(log)}")
        '''
        # Iterate through traces (cases)
        for case_id, this_case in log.items():
            print(f"Trace {case_id}:")
            print(this_case)
            # Iterate through events in each trace
            for index_event, this_event in enumerate(this_case):
                print(this_event)
                '''
    except  Exception as e: 
        print(e)
        return False
    return True


def getEvents():
    global events
    return events


def getTraces():
    global log
    return len(log)


def getColumns():
    global columns
    return columns


def convertTimestamp(iso_timestamp: str):
    return int((datetime.strptime(iso_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")).timestamp())


def compare(actual, op: str, expected):
    #print(actual+ " | " +op+ " | " +expected)
    comparison: bool = False
    try:
        actual = float(actual)
        expected = float(expected)
    except:
        actual = str(actual)
        expected = str(expected)
    if op == "is":
        comparison = (actual == expected)
    elif op == "isnot":
        comparison = (not (actual == expected))
    elif op == "=" or op == "==":
        comparison = (actual == expected)
    elif op == "!=":
        comparison = (not (actual == expected))
    elif op == "<":
        comparison = (actual < expected)
    elif op == "<=":
        comparison = (actual <= expected)
    elif op == ">":
        comparison = (actual > expected)
    elif op == ">=":
        comparison = (actual >= expected)
    return comparison


def compareRange(v1, op: str, v2, range):
    # print(v1)
    # print(type(v1))
    comparison: bool = False
    try:
        range = float(range)
        v1 = float(v1)
        v2 = float(v2)
    except:
        # v1 = str(v1)
        # v2 = str(v2)
        pass
    try:
        delta = (v1 - v2).total_seconds()
    except:
        delta = abs(v1 - v2)
    if op == "=":
        comparison = (delta == range)
    elif op == "!=":
        comparison = (not (delta == range))
    elif op == "<":
        comparison = (delta < range)
    elif op == "<=":
        comparison = (delta <= range)
    elif op == ">":
        comparison = (delta > range)
    elif op == ">=":
        comparison = (delta >= range)
    return comparison


def parametrizedValue(event, txId, value):
    toReturn = value
    if type(value) == str:
        if value.startswith(txId):
            path = value.split('.', 1)[1]
            try:
                if path.count('.') == 0:
                    toReturn = event[path]
                else:
                    # Split the path
                    keys = path.split(".")

                    # Navigate through the dictionary
                    sub_data = reduce(operator.getitem, keys[:-1], event)  # Get up to list

                    # Find the dictionary in the list where "name" matches the last key
                    if keys[-2] == "inputs":
                        toReturn = next(item["inputValue"] for item in ast.literal_eval(f"""{sub_data}""") if
                                        item["inputName"] == keys[-1])
                    elif keys[-2] == "storageState":
                        toReturn = next(item["variableValue"] for item in ast.literal_eval(f"""{sub_data}""") if
                                        item["variableName"] == keys[-1])
            except Exception as inst:
                print(type(inst))
                print(inst.args)
                print(inst)

    return toReturn

def checkEF(evA, comp, delta, unit, colTime, colBlock, evB):
    # print(evA)
    # print(comp)
    # print(delta)
    # print(unit)
    # print(colTime)
    # print(colBlock)
    # print(evB)
    if unit in "seconds":
        return compareRange(evB[colTime], comp, evA[colTime], delta)
    elif unit in "blocks":
        return compareRange(evB[colBlock], comp, evA[colBlock], delta)
    return False


def checkDF(evA, comp, delta, unit, colTime, colBlock, evB):
    pass

def checkNEF(evA, comp, delta, unit, colTime, colBlock, evB):
    pass

def checkNDF(evA, comp, delta, unit, colTime, colBlock, evB):
    pass


def applyBinaryRule(parsed: dict, mapping):
    compliant = []
    noncompliant = []

    # --- 1. Extract Rules dynamically ---
    tx_rules = []
    cf_rules = []
    
    i = 0
    while True:
        if f"tx{i}" in parsed: tx_rules.append(parsed[f"tx{i}"])
        else: break
        if f"cf{i}" in parsed: cf_rules.append(parsed[f"cf{i}"])
        i += 1
        
    for case_id, this_case in log.items():
        
        # --- 2. Find Constraints (The "Where") ---
        # We store the INDEX of the event that matches each tx rule
        found_indices = [None] * len(tx_rules)
        # print(f'before - {found_indices}')
        for tx_idx, tx_rule in enumerate(tx_rules):
            constraints = tx_rule["constraint"]
            tx_id = tx_rule.get("txId", "txId")
            
            for event_idx, event in enumerate(this_case):
                # Only look for the first match for this step
                if found_indices[tx_idx] is None: 
                    if check_full_constraint(event, constraints, mapping):
                        found_indices[tx_idx] = event_idx
                        break 

        # print(f'after - {found_indices}')
        # --- 3. Check Flow (The "When") ---
        is_case_compliant = True
        
        # Check every relationship (0->1, 1->2, etc.)
        for i, cf in enumerate(cf_rules):
            idx_A = found_indices[i]
            idx_B = found_indices[i+1]
            
            # If a transaction wasn't found, we can't check time/value logic
            # (Logic below handles "missing" events based on operator type)
            
            cf_type = cf["cfb"][0] # "er", "nef", etc.
            
            # RELATION: Eventual Follows (er/ef)
            if cf_type in ["er", "ef"]:
                #print(f'checking er... {idx_A} - {idx_B}')
                if idx_A is not None:
                    # If A happened, if MUST happen after B
                    if idx_B is None or idx_A < idx_B:
                        is_case_compliant = False
                    else:
                        # Check Time/Value constraints if defined
                        event_A = this_case[idx_A]
                        event_B = this_case[idx_B]
                        # Assuming you have checkEF/checkDF from your old code:
                        # if not checkEF(event_A, cf["comp"], cf["val"], cf["unit"], mapping.TIME, mapping.B, event_B):
                        #    is_case_compliant = False
                        pass
            elif cf_type in ["edr", "dr"]:
                #print(f'checking dr... {idx_A} - {idx_B}')
                if idx_A is not None:
                    # If A happened, it MUST happen immidiately after B
                    if idx_B is None or ((idx_A - idx_B) != 1):
                        is_case_compliant = False
                else:
                    if idx_B is not None:
                        is_case_compliant = False                

            # RELATION: Never Follows (nef)
            elif cf_type == "nef":
                 if idx_A is not None and idx_B is not None:
                     if idx_B > idx_A:
                         is_case_compliant = False

        if all(x is None for x in found_indices): pass
        elif is_case_compliant: 
            compliant.append(this_case) 
            #print("OK")
        else: 
            noncompliant.append(this_case)
            #print("---")

    return compliant, noncompliant

def check_full_constraint(event, constraints, mapping):
    """
    Router that directs checks to the specific helpers.
    Implicitly ANDs all top-level keys.
    """
    for key, condition in constraints.items():
        
        if key == "field_logic":
            # List of logic trees
            for logic_block in condition:
                if not check_logic_tree(logic_block, event, mapping): return False
        
        elif key == "attr":
             if not check_attr(condition, event, mapping): return False

        elif key == "call":
             if not check_call(condition, event, mapping): return False
                 
        else:
            # Simple field (function, block, etc.)
            if not check_flat_field(key, condition, event, mapping): return False
                
    return True

def applyUnaryRule(parsed: dict, mapping):
    compliant = []
    noncompliant = []
    
    tx_rule = parsed["tx0"]["constraint"]
    mode = parsed["cf0"]["cfu"][0]  # "occ" or "nocc"

    for case_id, this_case in log.items():
        found_tx = False
        found_index = None

        for event_idx, event in enumerate(this_case):
            # print(event)
            # Assume the event matches until proven otherwise (Implicit AND)
            match_all_constraints = True
            
            for key, condition in tx_rule.items():
                
                # Case A: Recursive Logic Tree (AND/OR)
                if key == "field_logic":
                    # condition is a list of logic trees
                    for logic_block in condition:
                        if not check_logic_tree(logic_block, event, mapping):
                            match_all_constraints = False
                            break
                
                # Case B: Attributes (State Variables / Inputs)
                elif key == "attr":
                    if not check_attr(condition, event, mapping):
                        match_all_constraints = False

                # Case C: Internal Calls
                elif key == "call":
                    if not check_call(condition, event, mapping):
                        match_all_constraints = False

                # Case D: Direct Simple Fields (The new requirement)
                # e.g., key="function", condition={"op": "==", "value": "value"}
                else: 
                    if not check_flat_field(key, condition, event, mapping):
                        match_all_constraints = False
                
                # Optimization: Stop checking this event if any constraint failed
                if not match_all_constraints:
                    break

            if match_all_constraints:
                # print("FOUNDD!")
                found_tx = True
                found_index = event_idx
                break
        
        # --- Compliance Decision ---
        if mode == 'occ':
            # print("valuto occ")
            if found_tx: compliant.append(this_case)
            else: noncompliant.append(this_case)
        elif mode == 'init':
            if found_tx and (found_index == 0): compliant.append(this_case)
            else: noncompliant.append(this_case)
        elif mode == 'end':
            if found_tx and (found_index == (len(this_case)-1)): compliant.append(this_case)
            else: noncompliant.append(this_case)
        else: # nocc
            if found_tx: noncompliant.append(this_case)
            else: compliant.append(this_case)

    return compliant, noncompliant

# ==========================================
# New Helper: Flat Field Checker
# ==========================================
def check_flat_field(field_name, condition, event, mapping):
    """
    Checks direct fields like 'function', 'block', 'sender'.
    condition: {"op": "==", "value": "val"}
    """
    #print(field_name + " | " + json.dumps(condition) + " | " + event[mapping.function])
    target_val = condition.get("value")
    # Support both 'val' and 'value' keys just in case
    if target_val is None: target_val = condition.get("val")

    # Map the field name to the event value
    actual_val = None
    if field_name == "function": actual_val = event[mapping.function]
    elif field_name == "contract": actual_val = event[mapping.contract]
    elif field_name == "block":  actual_val = event[mapping.block]
    elif field_name == "sender": actual_val = event[mapping.sender]
    elif field_name == "timestamp": actual_val = event[mapping.timestamp]
    elif field_name == "gasLimit": actual_val = event[mapping.gasLimit]
    elif field_name == "gasUsed": actual_val = event[mapping.gasUsed]
    elif field_name == "value": actual_val = event[mapping.value]
    
    ret = compare(actual_val, condition.get("op"), target_val)
    return ret

# ==========================================
# Helper: Recursive Logic (The "Tree" Walker)
# ==========================================
def check_logic_tree(node, event, mapping):
    """Recursively evaluates OR / AND logic."""
    
    # Base Case: It's a direct field check
    if "field" in node:
        field_name = node["field"]
        
        return check_flat_field(field_name , node, event, mapping)

    # Recursive Case: Logic Gates
    left = check_logic_tree(node["left"], event, mapping)
    right = check_logic_tree(node["right"], event, mapping)

    if node["op"] == "AND": return left and right
    if node["op"] == "OR":  return left or right
    return False


# ==========================================
# Helper: Attribute Checker (The "List" Walker)
# ==========================================
def check_attr(node, event, mapping):
    """Checks inside lists like State Variables (SV)."""
    target_type = node["type"] # e.g. "sv"
    cond = node["condition"]   # e.g. {field: sv1, value: value3}
    
    try:
        items = []
        if target_type == "sv":
            items = ast.literal_eval(str(event[mapping.SV]))
        elif target_type == "input":
            items = ast.literal_eval(str(event[mapping.I]))
        elif target_type == "event":
            items = ast.literal_eval(str(event[mapping.E]))

        # Check if ANY item in the list matches the condition
        for item in items:
            if evaluate_item_logic(cond, item, target_type):
                return True
        return False
            
            # if item.get(name_key) == cond["field"]:
            #     if str(item.get(val_key)) == str(cond["value"]):
            #         return True
            # elif target_type == "event":
            #     if cond["field"] == "name":
            #         if str(item.get("eventName")) == str(cond["value"]):
            #             return True
    except:
        return False
        
    return False

def evaluate_item_logic(node, item, target_type):
    """
    New Recursive Helper specifically for items inside a list.
    """

    # Note: Adjust 'variableName'/'inputName' based on your exact log keys
    name_key = "variableName" if target_type == "sv" else "inputName" if target_type == "input" else "eventName"
    val_key = "variableValue" if target_type == "sv" else "inputValue" if target_type == "input" else "eventValues"

    # 1. Base Case: Leaf Node (Actual check)
    if "field" in node:
        field = node["field"]
        comparator = node["op"]
        target_val = node["value"]
        
        # Extract actual value from the item dictionary
        item_val = None
        
        if target_type == "event":
            if field == "name": item_val = item.get("eventName")
            else: pass
            return compare(item_val, comparator, target_val)
            # Add logic for event parameters if needed
            
        elif target_type == "sv":
            if item.get(name_key) == field:
                return compare(item.get(val_key), comparator, target_val)
            #if field == "name": item_val = item.get("variableName") # Adjust based on your log
            #elif field == "value": item_val = item.get("variableValue")
            
        return False

    # 2. Recursive Case: Logic Gates
    left = evaluate_item_logic(node["left"], item, target_type)
    right = evaluate_item_logic(node["right"], item, target_type)

    if node["op"] == "AND": return left and right
    if node["op"] == "OR":  return left or right
    return False


# ==========================================
# Helper: Call Checker
# ==========================================
def check_call(node, event, mapping):
    """Checks inside the internal calls list."""
    target_fields = node["fields"] # e.g. { "function": {op:==, value: ...} }
    
    try:
        calls = ast.literal_eval(str(event[mapping.CALL]))
    except:
        return False
    #print(target_fields)
    for call in calls:
        #print(call)
        match_all = True
        for key, criteria in target_fields.items():
            #print(key+" | "+json.dumps(criteria))
            # Check if the call's field matches the requirement
            if str(call.get(key)) != str(criteria["value"]):
                match_all = False
                break
        
        if match_all:
            return True
            
    return False

def serialize(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, float) and np.isnan(obj):
        return None  # Replace NaN with None for JSON
    elif isinstance(obj, dict):
        return {k: serialize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [serialize(item) for item in obj]
    else:
        return obj


def verifyRule(rule: str, mapping):
    # print(rule)
    #parsed = interpretRule(rule, 0, 0)
    parsed: dict = json.loads(rule)
    #print(parsed)
    c, nc = [], []
    if (parsed.get("cf0", {}).get("cfb") is None):
        #print("Processing unary")
        c, nc = applyUnaryRule(parsed, mapping)
        #evaluateUnary(parsed, mapping)
    else:
        #print("Processing binary")
        c, nc = applyBinaryRule(parsed, mapping)
    # print(c)
    #  print(nc)
    # safe_data = serialize({"compliant": c, "noncompliant": nc})
    safe_data = jsonable_encoder({"compliant": c, "noncompliant": nc})

    return safe_data
