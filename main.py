import json
import subprocess
import logging
import boto3
import os
import os.path
import urllib.parse
import re
from typing import Union
from fastapi import FastAPI
from pydantic import BaseModel
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

# Creating the app
app = FastAPI()

# Setting up the logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

# AWS setup
s3_client = boto3.client("s3", region_name="ap-south-1")
transcribe_client = boto3.client("transcribe", region_name="ap-south-1")

# Models
class ProbeRequest(BaseModel):
    url: str

class ProbeResponse(BaseModel):
    success: bool
    has_audio: bool | None = None
    duration: float | None = None
    error: str | None = None

class ExtractAudioRequest(BaseModel):
    url:str

class ExtractAudioResponse(BaseModel):
    success : bool
    output_key: Union[str,None] = None
    transcription_job_name: Union[str, None] = None
    error: Union[str, None] = None

class TranscribeAudioRequest(BaseModel):
    audio_key: str
    bucket_name: str
    language_code: str = "en-US"  # Default to English

class TranscribeAudioResponse(BaseModel):
    success: bool
    transcription_job_name: Union[str, None] = None
    error: Union[str, None] = None
# Endpoints
@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/probe-audio", response_model = ProbeResponse)
async def probe_audio(req: ProbeRequest) -> ProbeResponse:
    logger.info("Detecting if the audio stream is there")
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
        logger.info(f"Detected audio - {has_audio},duration - {duration}")
        return ProbeResponse(success=True, has_audio=has_audio, duration=duration)

    except subprocess.CalledProcessError as e:
        logger.error(f"ffprobe failed - {e.stderr}")
        return ProbeResponse(success=False, error=f"ffprobe failed - {e.stderr}")

    except Exception as e:
        logger.error(f"ffprobe failed - {str(e)}")
        return ProbeResponse(success=False, error=str(e))


@app.post("/extract-audio", response_model=ExtractAudioResponse)
async def extract_audio(req: ExtractAudioRequest) -> ExtractAudioResponse:
    logger.info(f"Starting audio extraction for URL: {req.url}")
    temp_audio_file = None
    try:
        # Extract asset_id from URL (assuming file name is <asset_id>.mp4)
        parsed_url = urllib.parse.urlparse(req.url)
        file_name = os.path.basename(parsed_url.path)
        # Validate file name format and extract asset_id
        if not file_name.endswith('.mp4'):
            raise ValueError(f"Input file must be an .mp4 file, got: {file_name}")
        asset_id = re.match(r'^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.mp4$', file_name, re.I)
        if not asset_id:
            raise ValueError(f"File name must be a GUID followed by .mp4, got: {file_name}")
        asset_id = asset_id.group(1)  # Extract GUID
        temp_audio_file = f"/tmp/{asset_id}.mp3"

        # Extract audio with ffmpeg
        logger.info(f"Extracting audio to {temp_audio_file}")
        command = [
            "ffmpeg",
            "-i", req.url,
            "-vn",
            "-acodec", "mp3",
            "-y",
            temp_audio_file
        ]
        subprocess.run(command, capture_output=True, text=True, check=True)

        # Parse bucket name from URL
        bucket_name = None
        if parsed_url.netloc.endswith('.s3.amazonaws.com'):
            bucket_name = parsed_url.netloc.split('.')[0]
        elif parsed_url.scheme == 's3':
            bucket_name = parsed_url.path.split('/')[1]
        else:
            raise ValueError("Could not determine bucket name from URL")

        # Verify bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            logger.error(f"Bucket {bucket_name} does not exist or is inaccessible: {e}")
            raise ValueError(f"Invalid or inaccessible bucket: {bucket_name}")

        # Upload to S3
        audio_key = f"VOD/FinishedVideos/{asset_id}.mp3"
        logger.info(f"Uploading audio to S3: {bucket_name}/{audio_key}")
        s3_client.upload_file(
            temp_audio_file,
            bucket_name,
            audio_key,
            ExtraArgs={'ContentType': 'audio/mpeg'}
        )

        return ExtractAudioResponse(success=True, output_key=audio_key)

    except subprocess.CalledProcessError as e:
        logger.error(f"ffmpeg failed: {e.stderr}")
        return ExtractAudioResponse(success=False, error=f"Audio extraction failed: {e.stderr}")

    except (ClientError, NoCredentialsError, EndpointConnectionError) as e:
        logger.error(f"AWS S3 operation failed: {str(e)}")
        return ExtractAudioResponse(success=False, error=f"AWS S3 error: {str(e)}")

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return ExtractAudioResponse(success=False, error=f"Validation error: {str(e)}")

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return ExtractAudioResponse(success=False, error=f"Unexpected error: {str(e)}")

    finally:
        if os.path.exists(temp_audio_file):
            try:
                os.remove(temp_audio_file)
                logger.info(f"Cleaned up temporary file: {temp_audio_file}")
            except Exception as e:
                logger.error(f"Failed to clean up temporary file: {str(e)}")

@app.post("/transcribe-audio", response_model=TranscribeAudioResponse)
async def transcribe_audio(req: TranscribeAudioRequest) -> TranscribeAudioResponse:
    logger.info(f"Starting transcription for S3 key: {req.audio_key}")

    try:
        # Extract asset_id from audio_key (assuming format VOD/FinishedVideos/<asset_id>.mp3)
        file_name = os.path.basename(req.audio_key)
        asset_id = re.match(r'^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.mp3$', file_name, re.I)
        if not asset_id:
            raise ValueError(f"Audio file name must be a GUID followed by .mp3, got: {file_name}")
        asset_id = asset_id.group(1)  # Extract GUID
        transcription_job_name = f"transcription_{asset_id}"

        # Verify audio file exists in S3
        try:
            s3_client.head_object(Bucket=req.bucket_name, Key=req.audio_key)
        except ClientError as e:
            logger.error(f"S3 object {req.audio_key} does not exist or is inaccessible: {e}")
            return TranscribeAudioResponse(success=False, error=f"Invalid or inaccessible audio file: {req.audio_key}")

        # Start transcription job
        logger.info(f"Starting transcription job: {transcription_job_name}")
        transcribe_client.start_transcription_job(
            TranscriptionJobName=transcription_job_name,
            Media={'MediaFileUri': f"s3://{req.bucket_name}/{req.audio_key}"},
            MediaFormat='mp3',
            LanguageCode=req.language_code,
            OutputBucketName=req.bucket_name,
            OutputKey=f"VOD/Subtitles/{asset_id}.json"
        )

        return TranscribeAudioResponse(
            success=True,
            transcription_job_name=transcription_job_name
        )

    except (ClientError, NoCredentialsError, EndpointConnectionError) as e:
        logger.error(f"AWS Transcribe operation failed: {str(e)}")
        return TranscribeAudioResponse(success=False, error=f"AWS Transcribe error: {str(e)}")

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return TranscribeAudioResponse(success=False, error=f"Validation error: {str(e)}")

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return TranscribeAudioResponse(success=False, error=f"Unexpected error: {str(e)}")