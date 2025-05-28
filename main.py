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

class ProbeResponse(BaseModel):
    success: bool
    has_audio: bool | None = None
    duration: float | None = None
    error: str | None = None

@app.post("/probe-audio")
async def probe_audio(req: ProbeRequest):
    try:
        command = [
            "ffprobe",
            "-v", "error",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            req.url
        ]
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        output = json.loads(result.stdout)

        has_audio = any(stream.get("codec_type") == "audio" for stream in output.get("streams", []))
        duration = None
        # Prefer format duration, fallback to stream duration
        if "format" in output and "duration" in output["format"]:
            duration = float(output["format"]["duration"])
        elif output.get("streams"):
            for stream in output["streams"]:
                if "duration" in stream:
                    duration = float(stream["duration"])
                    break

        return ProbeResponse(success=True, has_audio=has_audio, duration=duration)

    except subprocess.CalledProcessError as e:
        return ProbeResponse(success=False, error=f"ffprobe failed - {e.stderr}")

    except Exception as e:
        return ProbeResponse(success=False, error=str(e))
