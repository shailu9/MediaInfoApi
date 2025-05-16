import json
import subprocess
from typing import Union
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


class ProbeRequest(BaseModel):
    url: str


@app.post("/probe-audio")
async def probe_audio(req: ProbeRequest):
    try:
        command = [
            "ffprobe",
            "-v", "error",
            "-print_format", "json",
            "-show_streams",
            req.url
        ]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        output = json.loads(result.stdout)

        has_audio = any(stream.get("codec_type") == "audio" for stream in output.get("streams", []))

        return {"success": True, "has_audio": has_audio}

    except subprocess.CalledProcessError as e:
        return {"success": False, "error": "ffprobe failed", "stderr": e.stderr}

    except Exception as e:
        return {"success": False, "error": str(e)}
