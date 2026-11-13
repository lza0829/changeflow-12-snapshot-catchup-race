from fastapi import APIRouter, HTTPException

from app.db import connect
from app.repositories import snapshots as snapshot_repo
from app.schemas.entities import CatchUpRequest, SnapshotImportRequest, SnapshotResetRequest
from app.services import snapshot_service

router = APIRouter(tags=["snapshots"])


@router.get("/snapshots/fixtures")
def fixtures():
    return snapshot_service.list_snapshot_fixtures()


@router.get("/snapshots/imports")
def list_imports():
    conn = connect()
    try:
        return snapshot_repo.list_imports(conn)
    finally:
        conn.close()


@router.get("/snapshots/imports/{import_id}")
def get_import(import_id: int):
    conn = connect()
    try:
        record = snapshot_repo.get_import(conn, import_id)
        if not record:
            raise HTTPException(404, "snapshot import not found")
        return record
    finally:
        conn.close()


@router.post("/snapshots/import")
def import_snapshot(body: SnapshotImportRequest):
    try:
        return snapshot_service.import_snapshot(body.stream_id, body.fixture_path)
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/snapshots/catchup")
def catch_up(body: CatchUpRequest):
    try:
        return snapshot_service.catch_up(body.stream_id, body.limit)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/snapshots/reset")
def reset_lane(body: SnapshotResetRequest):
    try:
        return snapshot_service.reset_lane(body.stream_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/snapshots/reconcile/{stream_id}")
def reconcile(stream_id: int):
    conn = connect()
    try:
        return snapshot_service.reconcile(conn, stream_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    finally:
        conn.close()
