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
from app.logic import checkEF


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
            c, nc, tc, tnc, ign = applyUnaryRuleLive(parsed, mapping, local_log_dict)
        else:
            c, nc, tc, tnc, ign = applyBinaryRuleLive(parsed, mapping, local_log_dict)
            
        safe_data = jsonable_encoder({
            "compliant": c, 
            "nonCompliant": nc,  
            "tempCompliant": tc, 
            "tempNonCompliant": tnc,
            "ignored": ign,})
        
        return safe_data
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

# funzioni che non prendono il log globale
def applyBinaryRuleLive(parsed: dict, mapping, local_log_dict: dict):
    compliant = []
    noncompliant = []
    tempComp = []
    tempNonComp = []
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

            # edr (exists direct response): Esiste ALMENO UN A immediatamente seguito da B
            elif cf_type == "edr":
                if not list_A:
                    trace_state = "TNC"
                else:
                    has_direct = any(((a + 1) in list_B) and is_valid_sequence(a, a + 1, cf) for a in list_A)
                    if not has_direct:
                        trace_state = "TNC"
                    # Se lo trova, trace_state rimane "C" (permanente)

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

    return compliant, noncompliant, tempComp, tempNonComp, ignored, 


def applyUnaryRuleLive(parsed: dict, mapping, local_log_dict: dict):
    compliant = []
    noncompliant = []
    ignored = []
    tempComp = []
    tempNonComp = []
    
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

