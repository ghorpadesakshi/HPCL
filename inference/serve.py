# inference/serve.py
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import os
from dotenv import load_dotenv

load_dotenv()

MODEL_NAME = os.getenv('MODEL_NAME', 'distilbert-base-uncased-finetuned-sst-2-english')
app = FastAPI(title="Inference Service")

print("Loading model:", MODEL_NAME)
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
model.eval()
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
model.to(device)
print("Model loaded on", device)

class BatchRequest(BaseModel):
    texts: List[str]

@app.post("/predict_batch")
async def predict_batch(req: BatchRequest):
    texts = req.texts
    # tokenization
    toks = tokenizer(texts, padding=True, truncation=True, return_tensors='pt')
    toks = {k: v.to(device) for k, v in toks.items()}
    with torch.no_grad():
        out = model(**toks)
        probs = torch.nn.functional.softmax(out.logits, dim=-1)
        probs_list = probs.cpu().numpy().tolist()
    # convert to label + score
    results = []
    for p in probs_list:
        # assuming label 0 = NEG, 1 = POS for SST-2 model
        label = "POS" if p[1] >= p[0] else "NEG"
        score = max(p)
        results.append({"label": label, "score": float(score)})
    return {"results": results}
