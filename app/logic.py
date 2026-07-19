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
import tempfile
from io import BytesIO
import os

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

        if file.lower().endswith('.csv'):
            data = pd.read_csv(file) 
        elif file.lower().endswith('.xes'):
            data = pm4py.read_xes(file)
        
        data = pm4py.read_xes(file)
        events = len(data)
        columns = data.columns.tolist()

        grouped = {}

        if "CryptoKitties" in file:
            grouped = data.groupby('case:ident:piid').apply(lambda x: x.to_dict(orient='records')).to_dict()
        elif 'case:case_id' in columns:
            grouped = data.groupby('case:case_id').apply(lambda x: x.to_dict(orient='records')).to_dict()
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


def applyBinaryRule(parsed: dict, mapping, log_dict: dict):
    compliant = []
    noncompliant = []
    ignored = []
    tempComp = []
    tempNonComp = []

    # --- 1. Extract Rules dynamically ---
    tx_rules = []
    cf_rules = []
    
    i = 0
    while True:
        if f"tx{i}" in parsed: tx_rules.append(parsed[f"tx{i}"])
        else: break
        if f"cf{i}" in parsed: cf_rules.append(parsed[f"cf{i}"])
        i += 1
        
    for case_id, this_case in log_dict.items():
        
        # lista multidimensione per verificare OGNI occorrenza degli eventi
        found_indices = [[] for _ in range(len(tx_rules))]
        for tx_idx, tx_rule in enumerate(tx_rules):
            constraints = tx_rule["constraint"]
            for event_idx, event in enumerate(this_case):
                if check_full_constraint(event, constraints, mapping):
                    found_indices[tx_idx].append(event_idx)

        # print(f'after - {found_indices}')
        # --- 3. Check Flow (The "When") ---
        trace_state = "C"
        
        # Funzione helper interna per validare anche il tempo se presente
        def is_valid_sequence(a_idx, b_idx, cf_node):
            # 1. Deve essere logicamente successivo
            if b_idx <= a_idx:
                return False
            # 2. Se c'è un vincolo temporale (unità), controllalo
            unit = cf_node.get("unit")
            if unit:
                evA = this_case[a_idx]
                evB = this_case[b_idx]
                return checkEF(evA, cf_node.get("comp"), cf_node.get("val"), unit, mapping.timestamp, mapping.block, evB)
            return True

        for i, cf in enumerate(cf_rules):
            list_A = found_indices[i]      
            list_B = found_indices[i+1]    
            cf_type = cf["cfb"][0] 
            
            # er: Esiste ALMENO UN A che è prima o poi seguito da un B
            if cf_type == "er":
                if not list_A:
                    trace_state = "TNC" # TODO: gestire tutti i casi in cui non ci sono le A
                else:
                    has_response = any(any(is_valid_sequence(a, b, cf) for b in list_B) for a in list_A)
                    if not has_response:
                        trace_state = "TNC" # Trovato A, manca B

            # r: OGNI A è prima o poi seguito da un B
            elif cf_type == "r":
                if not list_A:
                    if trace_state == "C": trace_state = "TC" # TODO
                else:
                    all_have_response = all(any(is_valid_sequence(a, b, cf) for b in list_B) for a in list_A)
                    if all_have_response:
                        if trace_state == "C": trace_state = "TC"
                    else:
                        trace_state = "TNC"

            # edr: Esiste ALMENO UN A immediatamente seguito da B
            elif cf_type == "edr":
                if not list_A:
                    trace_state = "TNC"
                else:
                    has_direct = any(((a + 1) in list_B) and is_valid_sequence(a, a + 1, cf) for a in list_A)
                    if not has_direct:
                        trace_state = "TNC"
                    # Se lo trova, trace_state rimane permanently compliant

            # dr: OGNI A è immediatamente seguito da B
            elif cf_type == "dr":
                if not list_A:
                    if trace_state == "C": trace_state = "TC"
                else:
                    is_tc = True
                    for a in list_A:
                        if ((a + 1) in list_B) and is_valid_sequence(a, a + 1, cf):
                            continue # Questo A specifico è conforme
                        else:
                            # Questo A non è seguito da un B valido. Capiamo il perché:
                            if a == len(this_case) - 1:
                                # A è l'ultimissimo evento della traccia. Potrebbe arrivare un B in futuro.
                                trace_state = "TNC"
                                is_tc = False
                            else:
                                # A è seguito da un evento, ma NON è B. La catena è rotta per sempre.
                                trace_state = "NC"
                                is_tc = False
                                break # Usciamo dal ciclo, la violazione irreversibile ha la priorità
                    
                    if is_tc:
                        if trace_state == "C": trace_state = "TC"
            
            # enr: Esiste ALMENO UN A che NON ha nessun B successivo
            elif cf_type == "enr":
                if not list_A:
                    trace_state = "TNC" # TODO: no A
                else:
                    # check if ANY 'a' has NO valid 'b' > 'a'
                    has_no_response = any(not any(is_valid_sequence(a, b, cf) for b in list_B) for a in list_A)
                    if has_no_response:
                        if trace_state == "C": trace_state = "TC" # Ne esiste uno, ma un B futuro potrebbe invalidarlo
                    else:
                        trace_state = "TNC" # Tutti gli A hanno un B. In attesa di un nuovo A.

            # nr: NESSUN A ha un B successivo (Ogni A è senza B)
            elif cf_type == "nr":
                if not list_A:
                    if trace_state == "C": trace_state = "TC" # TODO: verità vacua
                else:
                    # check if ANY 'a' has a valid 'b' > 'a'
                    has_violation = any(any(is_valid_sequence(a, b, cf) for b in list_B) for a in list_A)
                    if has_violation:
                        trace_state = "NC" # Trovato un A seguito da B. Violazione irreversibile.
                    else:
                        if trace_state == "C": trace_state = "TC" # Nessun A ha B, ma un B futuro potrebbe rovinare tutto
            
            # endr: Esiste ALMENO UN A il cui evento immediatamente successivo non è B
            elif cf_type == "endr":
                if not list_A:
                    trace_state = "TNC" # Nessun A presente
                else:
                    is_c = False
                    for a in list_A:
                        # Verifichiamo se A ha un evento successivo nella traccia
                        if a < len(this_case) - 1:
                            # Se l'evento successivo NON è un B valido (o non è in list_B o fallisce il tempo)
                            if not (((a + 1) in list_B) and is_valid_sequence(a, a + 1, cf)):
                                trace_state = "C" # Trovata la rottura diretta. Irreversibile.
                                is_c = True
                                break
                    
                    # Se non abbiamo trovato nessun A seguito da non-B (tutti seguiti da B, o A è ultimo)
                    if not is_c:
                        trace_state = "TNC"

            # ndr: OGNI A è seguito da qualcosa di diverso da B (o è l'ultimo evento)
            elif cf_type == "ndr":
                if not list_A:
                    if trace_state == "C": trace_state = "TC"
                else:
                    is_nc = False
                    for a in list_A:
                        if a < len(this_case) - 1:
                            # Se l'evento successivo è un B valido, violazione irreversibile
                            if ((a + 1) in list_B) and is_valid_sequence(a, a + 1, cf):
                                trace_state = "NC"
                                is_nc = True
                                break
                    
                    if not is_nc:
                        if trace_state == "C": trace_state = "TC"
            
            # wpr (weak pairwise response): L'n-esimo A è seguito dall'n-esimo B
            elif cf_type == "wpr":
                if not list_A:
                    if trace_state == "C": trace_state = "TC"
                else:
                    is_tc = True
                    for i in range(len(list_A)):
                        if i < len(list_B):
                            # Se l'n-esimo B viene prima dell'n-esimo A, o non rispetta il tempo
                            if list_A[i] >= list_B[i] or not is_valid_sequence(list_A[i], list_B[i], cf):
                                trace_state = "NC"
                                is_tc = False
                                break
                        else:
                            # Ci sono più A che B. Siamo in attesa del prossimo B.
                            trace_state = "TNC"
                            is_tc = False
                            break
                    if is_tc:
                        if trace_state == "C": trace_state = "TC"

            # spr (strong pairwise response): L'n-esimo A è seguito dall'n-esimo B senza altri A in mezzo
            elif cf_type == "spr":
                if not list_A:
                    if trace_state == "C": trace_state = "TC"
                else:
                    is_tc = True
                    for i in range(len(list_A)):
                        if i < len(list_B):
                            # Controllo appaiamento base
                            if list_A[i] >= list_B[i] or not is_valid_sequence(list_A[i], list_B[i], cf):
                                trace_state = "NC"
                                is_tc = False
                                break
                            # Controllo interferenza: c'è un A in mezzo?
                            if i + 1 < len(list_A) and list_A[i+1] < list_B[i]:
                                trace_state = "NC"
                                is_tc = False
                                break
                        else:
                            # Manca il B. Se abbiamo già registrato un altro A, la violazione è ormai inevitabile
                            if i + 1 < len(list_A):
                                trace_state = "NC"
                                is_tc = False
                                break
                            else:
                                trace_state = "TNC" # Manca solo il B, siamo in attesa
                                is_tc = False
                                break
                    if is_tc:
                        if trace_state == "C": trace_state = "TC"

            # c (choice): Almeno uno tra A e B deve essere eseguito
            elif cf_type == "c":
                if list_A or list_B:
                    trace_state = "C"
                else:
                    trace_state = "TNC"

            # ex (exclusive choice): Esattamente uno tra A e B deve essere eseguito, mai insieme
            elif cf_type == "ex":
                if list_A and list_B:
                    trace_state = "NC"
                elif list_A or list_B:
                    if trace_state == "C": trace_state = "TC"
                else:
                    trace_state = "TNC"

        if all(len(x) == 0 for x in found_indices): 
            ignored.append(this_case)
        elif trace_state == "C": 
            compliant.append(this_case) 
        elif trace_state == "NC": 
            noncompliant.append(this_case)
        elif trace_state == "TC": 
            tempComp.append(this_case)
        elif trace_state == "TNC": 
            tempNonComp.append(this_case)

    return compliant, noncompliant, tempComp, tempNonComp, ignored

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

def applyUnaryRule(parsed: dict, mapping, log_dict: dict):
    compliant = []
    noncompliant = []
    ignored = []
    tempComp = []
    tempNonComp = []
    
    tx_rule = parsed["tx0"]["constraint"]
    mode = parsed["cf0"]["cfu"][0]  # "occ" or "nocc"

    for case_id, this_case in log_dict.items():
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
            if found_tx:compliant.append(this_case)
            else: tempNonComp.append(this_case)
        elif mode == 'nocc': 
            if found_tx: noncompliant.append(this_case)
            else: tempComp.append(this_case)
        elif mode == 'init':
            if found_tx and (found_index == 0): compliant.append(this_case)
            else: noncompliant.append(this_case)
        elif mode == 'end':
            if found_tx and (found_index == (len(this_case)-1)): tempComp.append(this_case)
            else: tempNonComp.append(this_case)
    
    return compliant, noncompliant, tempComp, tempNonComp, ignored

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
    if field_name == "function": actual_val = event.get(mapping.function)
    elif field_name == "contract": actual_val = event.get(mapping.contract)
    elif field_name == "block":  actual_val = event.get(mapping.block)
    elif field_name == "sender": actual_val = event.get(mapping.sender)
    elif field_name == "timestamp": actual_val = event.get(mapping.timestamp)
    elif field_name == "gasLimit": actual_val = event.get(mapping.gasLimit)
    elif field_name == "gasUsed": actual_val = event.get(mapping.gasUsed)
    elif field_name == "value": actual_val = event.get(mapping.value)
    
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
    c, nc, tc, tnc, ign = [], [], [], [], []
    if (parsed.get("cf0", {}).get("cfb") is None):
        #print("Processing unary")
        c, nc, tc, tnc, ign = applyUnaryRule(parsed, mapping, log)
        #evaluateUnary(parsed, mapping)
    else:
        #print("Processing binary")
        c, nc, tc, tnc, ign = applyBinaryRule(parsed, mapping, log)
    # print(c)
    #  print(nc)
    # safe_data = serialize({"compliant": c, "noncompliant": nc})
    safe_data = jsonable_encoder({
        "compliant": c, 
        "noncompliant": nc,
        "tempCompliant": tc,
        "tempNonCompliant": tnc,
        "ignored": ign})

    return safe_data

def verifyRuleLive(xes_string: str, rule: str, mapping: Mapping):

    # creo un file temporaneo per leggere lo xes
    with tempfile.NamedTemporaryFile(mode='w', suffix='.xes', delete=False) as tmp:
        tmp.write(xes_string)
        tmp_path = tmp.name
    
    data = pm4py.read_xes(tmp_path)

    data = data.replace({np.nan: None})# sostituisco NaN con None per rendere compatibili i JSON

    columns = data.columns.tolist()

    try:
        # raggruppamento degli eventi
        if "CryptoKitties" in xes_string:
            grouped = data.groupby('case:ident:piid').apply(lambda x: x.to_dict(orient='records')).to_dict()
        elif 'case:case_id' in columns:
            grouped = data.groupby('case:case_id').apply(lambda x: x.to_dict(orient='records')).to_dict()
        else:
            grouped = data.groupby('case:concept:name').apply(lambda x: x.to_dict(orient='records')).to_dict()

        local_log_dict = {str(key): value for key, value in grouped.items()}

        # parsing regola
        parsed: dict = json.loads(rule)
        c, nc, ign, tc, tnc = [], [], [], [], []
        
        # verifica della regola
        if (parsed.get("cf0", {}).get("cfb") is None):
            c, nc, tc, tnc, ign = applyUnaryRule(parsed, mapping, local_log_dict)
        else:
            c, nc, tc, tnc, ign = applyBinaryRule(parsed, mapping, local_log_dict)
            
        safe_data = jsonable_encoder({
            "compliant": c, 
            "noncompliant": nc,  
            "tempCompliant": tc, 
            "tempNonCompliant": tnc,
            "ignored": ign})
        
        return safe_data
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

