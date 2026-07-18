import requests
import os
import pm4py
import tempfile
from fastapi import FastAPI, Request, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Dict, Any
from app import logic
from app import jsonConverter
from app import liveLogic


app = FastAPI(debug=os.environ.get("MODE", "DEBUG") == "DEBUG")
######################
UPLOAD_FOLDER = 'uploads' 
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Rule(BaseModel):
    rule: str

class conversionParameters(BaseModel):
    # input_path: str
    data: List[Dict[str, Any]]
    case_col: str
    activity_col: str
    time_col: str
    xes_name: str
    extract_columns: bool = False

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

class LiveVerificationRequest(BaseModel):
    xes_string: str
    rule: str
    mapping: Mapping

@app.post("/api/uploadLog")
async def uploadFile(file: UploadFile = File(...)):
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    with open(file_path, "wb") as f:
        f.write(file.file.read())
    if logic.uploadLog(file_path):
        if os.path.exists(file_path):
            os.remove(file_path)
        else:
            print("The file does not exist")
    return getLog(file.filename)

def getLog(log: str):
    traces = logic.getTraces()
    events = logic.getEvents()
    columns = logic.getColumns()
    return JSONResponse(content={"logName": log, "numTraces": traces, "numEvents": events, "columns": columns})

@app.post("/api/verifyRule")
async def verifyRule(rule: Rule, mapping: Mapping):
    #print(rule.rule)
    #print(mapping)
    # txId1( event( ( name == AuctionSuccessful OR name == AuctionCancelled))) er > 0 seconds txId2( function == createSaleAuction)
    # txId1( event( ( name == AuctionSuccessful OR name == AuctionCancelled))) er > 0 seconds txId2( (function == createSaleAuction OR function == AuctionCreated))
    res = logic.verifyRule(rule.rule, mapping)
    #print(type(res))
    #print(res)
    return JSONResponse(content=res)

@app.post("/api/verifyRuleLive")
async def verifyRuleLive(request: LiveVerificationRequest):
    try:
        res = liveLogic.verifyRuleLive(
            request.xes_string, 
            request.rule, 
            request.mapping
        )
        return JSONResponse(content=res)
        
    except Exception as e:
        print(f"Errore critico durante verifyRuleLive: {e}")
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})
    

@app.post("/api/convertToXes")
async def convertJson(conversionParamenters: conversionParameters):
    
    try:
        # se il payload è vuoto, ritorno uno xes senza tracce
        if not conversionParamenters.data or len(conversionParamenters.data) == 0:
            empty_xes_lines = [
                '<log xes.version="1.0" xes.features="nested-attributes">',
                f'<string key="concept:name" value="{conversionParamenters.xes_name}"/>',
                '</log>'
            ]
            empty_xes_string = "\n".join(empty_xes_lines)
            
            return { 
                "success": True, 
                "xes_string": empty_xes_string,
                "columns": []
            }
        

        dataset = jsonConverter.load_dataset(conversionParamenters.data)

        xesContent = jsonConverter.build_log_content(
            dataset,
            conversionParamenters.case_col,
            conversionParamenters.activity_col,
            conversionParamenters.time_col)
        
        xesLogString = jsonConverter.generate_xes(xesContent, conversionParamenters.xes_name)

        columns_list = []
        if conversionParamenters.extract_columns:
            # creazione file temporaneo per leggere le colonne direttamente
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xes", mode="w", encoding="utf-8") as temp_file:
                temp_file.write(xesLogString)
                temp_file_path = temp_file.name
            
            try:
                data = pm4py.read_xes(temp_file_path)
                columns_list = data.columns.tolist()
            finally:
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)

        # output_path = f"uploads/{conversionParamenters.xes_name}.xes"
        # with open(output_path, "w", encoding="utf-8") as f:
        #     f.write(xesLogString)

        return { 
            "success": True, 
            "xes_string": xesLogString,
            "columns": columns_list 
        }
    
    except Exception as e:
        print(f"Errore durante la conversione in XES: {e}")
        return {"success": False, "error": str(e)}


if not app.debug:
    static_files_folder = os.path.join(os.path.dirname(__file__), 'static')
    if os.path.exists(static_files_folder) and os.path.isdir(static_files_folder):
        app.mount("/", StaticFiles(directory=static_files_folder, html=True), name="static")
