from pydantic import BaseModel


class ApplyRequest(BaseModel):
    stream_id: int


class StreamPatch(BaseModel):
    enabled: bool | None = None


class SettingPut(BaseModel):
    value: str


class SnapshotImportRequest(BaseModel):
    stream_id: int
    fixture_path: str


class CatchUpRequest(BaseModel):
    stream_id: int
    limit: int | None = None


class SnapshotResetRequest(BaseModel):
    stream_id: int
