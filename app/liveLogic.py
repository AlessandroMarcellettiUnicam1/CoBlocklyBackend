import json
import pm4py
from io import BytesIO
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from app.logic import check_full_constraint
import tempfile
import os


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

def verifyRuleLive(xes_string: str, rule: str, mapping):

    # creo un file temporaneo per leggere lo xes
    with tempfile.NamedTemporaryFile(mode='w', suffix='.xes', delete=False) as tmp:
        tmp.write(xes_string)
        tmp_path = tmp.name
    
    data = pm4py.read_xes(tmp_path)
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
        c, nc, ign = [], [], []
        
        # verifica della regola
        if (parsed.get("cf0", {}).get("cfb") is None):
            c, nc, ign = applyUnaryRuleLive(parsed, mapping, local_log_dict)
        else:
            c, nc, ign = applyBinaryRuleLive(parsed, mapping, local_log_dict)
            
        safe_data = jsonable_encoder({"compliant": c, "noncompliant": nc, "ignored": ign})
        return safe_data
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

# funzioni che non prendono il log globale
def applyBinaryRuleLive(parsed: dict, mapping, local_log_dict: dict):
    compliant = []
    noncompliant = []
    ignored = []

    tx_rules = []
    cf_rules = []
    
    i = 0
    while True:
        if f"tx{i}" in parsed: tx_rules.append(parsed[f"tx{i}"])
        else: break
        if f"cf{i}" in parsed: cf_rules.append(parsed[f"cf{i}"])
        i += 1
        
    for case_id, this_case in local_log_dict.items():
        found_indices = [None] * len(tx_rules)
        for tx_idx, tx_rule in enumerate(tx_rules):
            constraints = tx_rule["constraint"]
            
            for event_idx, event in enumerate(this_case):
                if found_indices[tx_idx] is None: 
                    # Chiamata alla funzione pura importata
                    if check_full_constraint(event, constraints, mapping):
                        found_indices[tx_idx] = event_idx
                        break 

        is_case_compliant = True
        
        for i, cf in enumerate(cf_rules):
            idx_A = found_indices[i]
            idx_B = found_indices[i+1]
            cf_type = cf["cfb"][0] 
            
            if cf_type in ["er", "ef"]:
                if idx_A is not None:
                    if idx_B is None or idx_A < idx_B:
                        is_case_compliant = False
            elif cf_type in ["edr", "dr"]:
                if idx_A is not None:
                    if idx_B is None or ((idx_A - idx_B) != 1):
                        is_case_compliant = False
                else:
                    if idx_B is not None:
                        is_case_compliant = False                
            elif cf_type == "nef":
                 if idx_A is not None and idx_B is not None:
                     if idx_B > idx_A:
                         is_case_compliant = False

        if all(x is None for x in found_indices): 
            ignored.append(this_case)
        elif is_case_compliant: compliant.append(this_case) 
        else: noncompliant.append(this_case)

    return compliant, noncompliant


def applyUnaryRuleLive(parsed: dict, mapping, local_log_dict: dict):
    compliant = []
    noncompliant = []
    ignored = []
    
    tx_rule = parsed["tx0"]["constraint"]
    mode = parsed["cf0"]["cfu"][0] 

    for case_id, this_case in local_log_dict.items():
        found_tx = False
        found_index = None

        for event_idx, event in enumerate(this_case):
            if check_full_constraint(event, tx_rule, mapping):
                found_tx = True
                found_index = event_idx
                break
        
        if mode == 'occ':
            if found_tx: compliant.append(this_case)
            else: noncompliant.append(this_case)
        elif mode == 'init':
            if found_tx and (found_index == 0): compliant.append(this_case)
            else: noncompliant.append(this_case)
        elif mode == 'end':
            if found_tx and (found_index == (len(this_case)-1)): compliant.append(this_case)
            else: noncompliant.append(this_case)
        else: 
            if found_tx: noncompliant.append(this_case)
            else: compliant.append(this_case)

    return compliant, noncompliant, ignored

