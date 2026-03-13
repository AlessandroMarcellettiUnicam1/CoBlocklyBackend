import requests
import os
from fastapi import FastAPI, Request, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app import logic


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

if not app.debug:
    static_files_folder = os.path.join(os.path.dirname(__file__), 'static')
    if os.path.exists(static_files_folder) and os.path.isdir(static_files_folder):
        app.mount("/", StaticFiles(directory=static_files_folder, html=True), name="static")
