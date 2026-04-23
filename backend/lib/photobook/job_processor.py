import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Any, Coroutine, cast

import aiofiles
import aiohttp
from google import genai
from google.genai import types

from backend.lib.asset_manager.factory import AssetManagerFactory


class JobProcessor:
    def __init__(self, job_id: str, job_data: dict[str, Any]) -> None:
        self.job_id = job_id
        self.job_data = job_data
        self.image_keys: list[str] = job_data.get("image_keys", [])
        self.instruction: str = job_data.get("instruction", "")
        self.asset_manager = AssetManagerFactory.create()
        self._semaphore = asyncio.Semaphore(10)

        self.model = "gemini-2.5-flash-lite-preview-06-17"
        self.client = genai.Client(
            vertexai=True, project="perfect-aura-462400-f5", location="global"
        )

    async def _download_image(self, key: str, dest_path: Path) -> Path:
        async with self._semaphore:
            signed_url = await self.asset_manager.generate_signed_url(key)
            async with aiohttp.ClientSession() as session:
                async with session.get(signed_url) as resp:
                    resp.raise_for_status()
                    async with aiofiles.open(dest_path, "wb") as f:
                        await f.write(await resp.read())
            return dest_path

    async def _download_all_images(self, tmpdir_path: Path) -> list[Path]:
        tasks: list[Coroutine[Any, Any, Path]] = []
        for key in self.image_keys:
            dest = tmpdir_path / Path(key).name
            tasks.append(self._download_image(key, dest))
        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful: list[Path] = []
        for key, result in zip(self.image_keys, results):
            if isinstance(result, Exception):
                logging.error(f"[{self.job_id}] Failed to download {key}: {result}")
            else:
                successful.append(Path(cast("str", result)))
        return successful

    def _build_gemini_content(self, image_paths: list[Path]) -> list[types.Content]:
        parts: list[types.Part] = []

        # Build structured prompt content with image parts
        parts.append(types.Part.from_text(text="<request>\n<photos>\n"))

        for idx, path in enumerate(image_paths, 1):
            with open(path, "rb") as f:
                raw_bytes = f.read()
            image_part = types.Part.from_bytes(data=raw_bytes, mime_type="image/png")

            parts.append(types.Part.from_text(text=f"<photo><id>{idx}</id><img>"))
            parts.append(image_part)
            parts.append(types.Part.from_text(text="</img></photo>\n"))

        parts.append(types.Part.from_text(text="</photos>\n<instruction>\n"))
        parts.append(types.Part.from_text(text=self.instruction))
        parts.append(types.Part.from_text(text="\n</instruction>\n</request>"))

        return [types.Content(role="user", parts=parts)]

    def _build_config(self) -> types.GenerateContentConfig:
        sys_prompt = """The user will give you a structured XML like request that specifies some photos (n = 1 - 100) and their metadata, as well as some instructions, such as
<request>
  <photos>
  <photo><id>1</id><img>[image bytes]</img></photo>
  <photo><id>2</id><img>[image bytes]</img></photo>
  <photo><id>3</id><img>[image bytes]</img></photo>
  </photos>
  <instruction>
    I'm creating a photo book to celebrate a memory with my girlfriend. 
  </instruction>
</request>

With the request, the user is trying to create a photobook. Use all that you can infer from the uploaded photos and do the following.
    1. Group the photos into pages. Each page can have 1-6 photos.  You should group by subject, location, time, or anything you see fit. Each page should have a meaningful and coherent theme.
    2. For each page, optionally write a message in 1-3 sentences to celebrate the occasion identified by the photos you chose on that page if you see fit. Tone: Casual, celebratory, romantic, don't use words too fancy; Remember: The message should sound super natural as if the user is trying to convey the message to the photobook viewer. 

To recap, your job is to understand the user instructions, identify the grouping and return an XML in the following example format:
<response>
<page>
    <photo><id>1</id><img>[message]</img></photo>
    <photo><id>2</id><img>[message]</img></photo:
</page>

<page>
    <photo><id>3</id><img>[message]</img></photo>
    <photo><id>5</id><img>[message]</img></photo>
</page>
</response>"""

        return types.GenerateContentConfig(
            temperature=1.0,
            top_p=0.95,
            max_output_tokens=65535,
            safety_settings=[
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                    threshold=types.HarmBlockThreshold.OFF,
                ),
            ],
            system_instruction=[types.Part.from_text(text=sys_prompt)],
            thinking_config=types.ThinkingConfig(thinking_budget=0),
        )

    async def _run_gemini(self, image_paths: list[Path]) -> str:
        contents = self._build_gemini_content(image_paths)
        config = self._build_config()

        # Stream and collect output
        chunks = self.client.models.generate_content_stream(
            model=self.model,
            contents=contents,
            config=config,
        )
        response_text = ""
        for chunk in chunks:
            if chunk.text is not None:
                response_text += chunk.text
        logging.info(f"{response_text}")
        return response_text

    async def process(self) -> dict[str, Any]:
        if not self.image_keys:
            raise ValueError("No image_keys found in job_data")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            downloaded_paths = await self._download_all_images(tmp_path)
            if not downloaded_paths:
                raise RuntimeError("All image downloads failed")

            try:
                gemini_output = await self._run_gemini(downloaded_paths)
            except Exception as e:
                gemini_output = f"Gemini generation failed: {e}"

        return {
            "job_id": self.job_id,
            "processed_keys": self.image_keys,
            "successful_files": [str(p) for p in downloaded_paths],
            "gemini_result": gemini_output,
        }
