import asyncio
import logging
import uuid
from pathlib import Path
from typing import Optional

from fastapi import File, UploadFile
from pydantic import BaseModel

from backend.lib.utils import TempDirManager
from backend.route_handler.base import RouteHandler


class UploadedFileInfo(BaseModel):
    filename: str
    storage_key: str


class FailedUploadInfo(BaseModel):
    filename: str
    error: str


class NewPhotobookResponse(BaseModel):
    job_id: str
    uploaded_files: list[UploadedFileInfo]
    failed_uploads: list[FailedUploadInfo]
    skipped_non_media: list[str]


class TimelensAPIHandler(RouteHandler):
    def register_routes(self) -> None:
        self.router.add_api_route(
            "/api/new_photobook",
            self.new_photobook,
            methods=["POST"],
            response_model=NewPhotobookResponse,
        )

    @staticmethod
    def is_accepted_mime(mime: Optional[str]) -> bool:
        return mime is not None and (
            mime.startswith("image/")
            # or mime.startswith("video/") # only images allowed for now
        )

    async def new_photobook(
        self, files: list[UploadFile] = File(...)
    ) -> NewPhotobookResponse:
        job_id = f"job_{uuid.uuid4().hex}"

        # Filter valid files
        valid_files = [
            file
            for file in files
            if TimelensAPIHandler.is_accepted_mime(file.content_type)
        ]
        file_names = [file.filename for file in valid_files]
        skipped = [
            file.filename
            for file in files
            if file not in valid_files and file.filename is not None
        ]
        logging.info({"accepted_files": file_names, "skipped_non_media": skipped})

        success: list[UploadedFileInfo] = []
        failed_uploads: list[FailedUploadInfo] = []

        async with TempDirManager(valid_files) as file_map:

            async def safe_upload(
                file_id: int, original_name: str, path: Path, mime: str
            ) -> None:
                key = f"uploads/{job_id}/{path.name}"
                try:
                    await self.app.asset_manager.upload_file(
                        file_path=str(path),
                        key=key,
                        content_type=mime,
                    )
                    success.append(
                        UploadedFileInfo(filename=original_name, storage_key=key)
                    )
                except Exception as e:
                    msg = f"Failed to upload {original_name} → {key}: {e}"
                    logging.warning(msg)
                    failed_uploads.append(
                        FailedUploadInfo(filename=original_name, error=str(e))
                    )

            await asyncio.gather(
                *[
                    safe_upload(file_id, original_name, path, mime)
                    for file_id, (original_name, (path, mime)) in enumerate(
                        file_map.items()
                    )
                ]
            )

        await self.app.job_manager.enqueue(
            job_id, [file.storage_key for file in success]
        )

        return NewPhotobookResponse(
            job_id=job_id,
            uploaded_files=success,
            failed_uploads=failed_uploads,
            skipped_non_media=skipped,
        )
