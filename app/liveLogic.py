import json
import pm4py
from io import BytesIO
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from app.logic import check_full_constraint
import tempfile
import os
import pandas as pd
import numpy as np


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

# TODO: aggiungere logica per il temporarily compliant e non compliant
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
            c, nc, ign = applyUnaryRuleLive(parsed, mapping, local_log_dict)
        else:
            c, nc, ign = applyBinaryRuleLive(parsed, mapping, local_log_dict)
            
        safe_data = jsonable_encoder({"compliant": c, "noncompliant": nc, "ignored": ign, "tempCompliant": tc, "tempNonCompliant": tnc})
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
        found_indices = [[] for _ in range(len(tx_rules))]
        for tx_idx, tx_rule in enumerate(tx_rules):
            constraints = tx_rule["constraint"]
            for event_idx, event in enumerate(this_case):
                if check_full_constraint(event, constraints, mapping):
                    found_indices[tx_idx].append(event_idx)

        is_case_compliant = True
        
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
            
            if cf_type == "er":
                if not list_A:
                    is_case_compliant = False
                else:
                    # check if ANY 'a' has a 'b' > 'a' AND matches time
                    has_response = any(any(is_valid_sequence(a, b, cf) for b in list_B) for a in list_A)
                    if not has_response:
                        is_case_compliant = False

            elif cf_type == "r":
                if list_A:
                    all_have_response = all(any(is_valid_sequence(a, b, cf) for b in list_B) for a in list_A)
                    if not all_have_response:
                        is_case_compliant = False

            elif cf_type == "edr":
                if not list_A:
                    is_case_compliant = False
                else:
                    # a+1 deve essere in list_B e rispettare il tempo
                    has_direct = any(((a + 1) in list_B) and is_valid_sequence(a, a + 1, cf) for a in list_A)
                    if not has_direct:
                        is_case_compliant = False                

            elif cf_type == "dr":
                if list_A:
                    all_have_direct = all(((a + 1) in list_B) and is_valid_sequence(a, a + 1, cf) for a in list_A)
                    if not all_have_direct:
                        is_case_compliant = False
            
            elif cf_type == "enr":
                if not list_A:
                    is_case_compliant = False
                else:
                    # check if ANY 'a' has NO valid 'b' > 'a'
                    has_no_response = any(not any(is_valid_sequence(a, b, cf) for b in list_B) for a in list_A)
                    if not has_no_response:
                        is_case_compliant = False

            elif cf_type == "nr":
                if list_A:
                    # check if ALL 'a' have NO valid 'b' > 'a'
                    none_have_response = all(not any(is_valid_sequence(a, b, cf) for b in list_B) for a in list_A)
                    if not none_have_response:
                        is_case_compliant = False

        # Verifica log ignorati (le liste sono tutte vuote)
        if all(len(x) == 0 for x in found_indices): 
            ignored.append(this_case)
        elif is_case_compliant: 
            compliant.append(this_case) 
        else: 
            noncompliant.append(this_case)

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
        elif mode == 'nocc': 
            if found_tx: noncompliant.append(this_case)
            else: compliant.append(this_case)

    return compliant, noncompliant, ignored

