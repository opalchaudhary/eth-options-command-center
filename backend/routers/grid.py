from decimal import Decimal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from grid_bot.config import DEFAULT_RISK_THRESHOLDS
from grid_bot.continuous_worker import gridbot_live_state, start_continuous_gridbot_worker
from grid_bot.delta_testnet_client import DeltaTestnetClient
from grid_bot.durable_lifecycle import DurableGridBotLifecycle
from grid_bot.engine import engine
from grid_bot.models import GridConfig, GridType, SpacingType, new_id, to_record_dict
from grid_bot.repository import repository
from grid_bot.rest_fallback import RestFallbackPoller
from grid_bot.supabase_repository import SupabaseGridRepository


router = APIRouter(prefix="/api/grid", tags=["gridbot"])


class GridCreateRequest(BaseModel):
    bot_name: str
    product_symbol: str = "ETHUSD"
    grid_type: GridType
    lower_price: Decimal
    upper_price: Decimal
    grid_count: int
    spacing_type: SpacingType
    lot_size: Decimal
    max_inventory_lots: Decimal
    allocated_capital: Decimal | None = None
    risk_capital: Decimal | None = None
    initial_inventory: Decimal = Decimal("0")
    notes: str | None = None
    risk_thresholds: dict | None = None


class StartRequest(BaseModel):
    reference_price: Decimal
    tick_size: Decimal = Decimal("0.1")


class StopRequest(BaseModel):
    reason: str = "manual"
    operator_notes: str | None = None


class RegridRequest(BaseModel):
    reason: str = "manual"


class EditGridRequest(BaseModel):
    reason: str = "manual_edit"
    grid_type: GridType | None = None
    lower_price: Decimal | None = None
    upper_price: Decimal | None = None
    grid_count: int | None = None
    spacing_type: SpacingType | None = None
    lot_size: Decimal | None = None
    max_inventory_lots: Decimal | None = None


class OperatorGridRequest(BaseModel):
    bot_name: str = "ETH Testnet Grid"
    product_symbol: str = "ETHUSD"
    grid_type: GridType = GridType.NEUTRAL
    lower_price: Decimal
    upper_price: Decimal
    grid_count: int = 4
    spacing_type: SpacingType = SpacingType.ARITHMETIC
    lot_size: Decimal = Decimal("1")
    max_inventory_lots: Decimal = Decimal("2")


def _model_payload(model: BaseModel) -> dict:
    if hasattr(model, "model_dump"):
        return model.model_dump(mode="json")
    return model.dict()


def _config_from_request(payload: GridCreateRequest) -> GridConfig:
    allocated_capital = payload.allocated_capital or Decimal("1")
    risk_capital = payload.risk_capital or allocated_capital
    return GridConfig(
        bot_id=new_id("bot"),
        config_version=1,
        bot_name=payload.bot_name,
        product_symbol=payload.product_symbol,
        grid_type=payload.grid_type,
        lower_price=payload.lower_price,
        upper_price=payload.upper_price,
        grid_count=payload.grid_count,
        spacing_type=payload.spacing_type,
        lot_size=payload.lot_size,
        max_inventory_lots=payload.max_inventory_lots,
        allocated_capital=allocated_capital,
        risk_capital=risk_capital,
        initial_inventory=payload.initial_inventory,
        notes=payload.notes,
        risk_thresholds={**DEFAULT_RISK_THRESHOLDS, **(payload.risk_thresholds or {})},
    )


@router.post("/bots")
def create_bot(payload: GridCreateRequest):
    try:
        config = engine.create_bot(_config_from_request(payload))
        return {"ok": True, "bot": to_record_dict(config)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/bots")
def list_bots():
    return {"ok": True, "bots": [to_record_dict(bot) for bot in repository.bots.values()]}


@router.get("/bots/{bot_id}")
def get_bot(bot_id: str):
    bot = repository.bots.get(bot_id)
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found.")
    return {"ok": True, "bot": to_record_dict(bot)}


@router.post("/bots/{bot_id}/preview")
def preview_bot(bot_id: str, payload: StartRequest):
    bot = repository.bots.get(bot_id)
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found.")
    return {"ok": True, "preview": engine.preview(bot, payload.reference_price, payload.tick_size)}


@router.post("/bots/{bot_id}/start")
def start_bot(bot_id: str, payload: StartRequest):
    if bot_id not in repository.bots:
        raise HTTPException(status_code=404, detail="Bot not found.")
    try:
        return engine.start(bot_id, payload.reference_price, payload.tick_size)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/{run_id}/pause")
def pause_run(run_id: str):
    if run_id not in repository.runs:
        raise HTTPException(status_code=404, detail="Run not found.")
    return {"ok": True, "run": engine.pause(run_id)}


@router.post("/bots/{bot_id}/pause")
def pause_bot(bot_id: str):
    run = repository.active_run()
    if not run or run.bot_id != bot_id:
        raise HTTPException(status_code=404, detail="Active run not found for bot.")
    return {"ok": True, "run": engine.pause(run.run_id)}


@router.post("/runs/{run_id}/resume")
def resume_run(run_id: str):
    if run_id not in repository.runs:
        raise HTTPException(status_code=404, detail="Run not found.")
    return {"ok": True, "run": engine.resume(run_id)}


@router.post("/bots/{bot_id}/resume")
def resume_bot(bot_id: str):
    run = repository.active_run()
    if not run or run.bot_id != bot_id:
        raise HTTPException(status_code=404, detail="Active run not found for bot.")
    return {"ok": True, "run": engine.resume(run.run_id)}


@router.post("/runs/{run_id}/stop")
def stop_run(run_id: str, payload: StopRequest):
    if run_id not in repository.runs:
        raise HTTPException(status_code=404, detail="Run not found.")
    return {"ok": True, **engine.stop(run_id, payload.reason, payload.operator_notes)}


@router.post("/bots/{bot_id}/stop")
def stop_bot(bot_id: str, payload: StopRequest):
    run = repository.active_run()
    if not run or run.bot_id != bot_id:
        raise HTTPException(status_code=404, detail="Active run not found for bot.")
    return {"ok": True, **engine.stop(run.run_id, payload.reason, payload.operator_notes)}


@router.get("/runs")
def list_runs():
    return {"ok": True, "runs": [to_record_dict(run) for run in repository.runs.values()]}


@router.get("/runs/{run_id}")
def get_run(run_id: str):
    run = repository.runs.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")
    return {"ok": True, "run": to_record_dict(run)}


@router.get("/runs/{run_id}/summary")
def get_summary(run_id: str):
    summary = repository.summaries.get(run_id)
    if not summary:
        raise HTTPException(status_code=404, detail="Summary not found.")
    return {"ok": True, "summary": summary}


@router.get("/runs/{run_id}/execution-mode")
def get_execution_mode(run_id: str):
    state = repository.rest_fallback_state.get(run_id)
    if not state:
        return {
            "ok": True,
            "execution_event_mode": "PRIVATE_WS",
            "operational_state": "NORMAL",
            "private_ws_status": "UNKNOWN",
        }
    return {"ok": True, **state}


@router.post("/runs/{run_id}/rest-fallback/activate")
def activate_rest_fallback(run_id: str):
    run = repository.runs.get(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found.")
    bot = repository.bots[run.bot_id]
    try:
        client = DeltaTestnetClient()
        spec = client.product_spec(bot.product_symbol)
        poller = RestFallbackPoller(client)
        state = poller.activate(run_id, spec.product_id, spec.symbol)
        return {"ok": True, "state": poller.serialise_state(state)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/bots/{bot_id}/events")
def bot_events(bot_id: str):
    return {"ok": True, "events": [event for event in repository.events if event.get("bot_id") == bot_id]}


@router.get("/v01/live/status")
def durable_live_status():
    try:
        return DurableGridBotLifecycle().status()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/v01/live/market-account")
def durable_market_account(product_symbol: str = "ETHUSD"):
    try:
        return DurableGridBotLifecycle().product_account_health(product_symbol)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/v01/live/preview")
def durable_preview_operator_grid(payload: OperatorGridRequest):
    try:
        return DurableGridBotLifecycle().preview_operator_grid(_model_payload(payload))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/v01/live/start")
def durable_start_operator_grid(payload: OperatorGridRequest):
    try:
        result = DurableGridBotLifecycle().start_operator_grid_background(_model_payload(payload))
        start_continuous_gridbot_worker()
        return result
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/v01/live/preview-tiny")
def durable_preview_tiny_grid():
    try:
        return DurableGridBotLifecycle().preview_tiny_grid()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/v01/live/start-tiny")
def durable_start_tiny_grid():
    try:
        result = DurableGridBotLifecycle().start_tiny_grid()
        start_continuous_gridbot_worker()
        return result
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/v01/live/reconcile")
def durable_reconcile():
    try:
        return DurableGridBotLifecycle().reconcile(process_replacements=True)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/v01/live/pause")
def durable_pause():
    try:
        return DurableGridBotLifecycle().pause()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/v01/live/resume")
def durable_resume():
    try:
        result = DurableGridBotLifecycle().resume()
        start_continuous_gridbot_worker()
        return result
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/v01/live/regrid")
def durable_regrid(_payload: RegridRequest | None = None):
    try:
        return DurableGridBotLifecycle().regrid()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/v01/live/edit/preview")
def durable_edit_preview(payload: EditGridRequest):
    try:
        data = _model_payload(payload)
        data.pop("reason", None)
        return DurableGridBotLifecycle().preview_edit_grid(payload=data)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/v01/live/edit")
def durable_edit(payload: EditGridRequest):
    try:
        data = _model_payload(payload)
        reason = data.pop("reason", "manual_edit")
        result = DurableGridBotLifecycle().edit_grid(payload=data, reason=reason)
        if (result.get("run") or {}).get("status") == "RUNNING":
            start_continuous_gridbot_worker()
        return result
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/v01/live/stop")
def durable_stop(payload: StopRequest):
    try:
        return DurableGridBotLifecycle().stop(reason=payload.reason)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/v01/live/state")
def durable_live_state():
    try:
        return gridbot_live_state()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/v01/history/{table_name}")
def durable_history(table_name: str, run_id: str | None = None, limit: int = 100):
    allowed = {
        "grid_runs",
        "grid_config_versions",
        "grid_levels",
        "grid_order_proposals",
        "grid_orders",
        "grid_fills",
        "grid_cycles",
        "grid_exchange_costs",
        "grid_bot_snapshots",
        "grid_risk_snapshots",
        "grid_parameter_changes",
        "grid_events",
        "grid_run_summaries",
    }
    if table_name not in allowed:
        raise HTTPException(status_code=404, detail="GridBot history table not exposed.")
    try:
        repo = SupabaseGridRepository()
        params = {"select": "*", "limit": max(1, min(int(limit), 500))}
        if run_id:
            params["run_id"] = f"eq.{run_id}"
        if table_name in {"grid_events", "grid_bot_snapshots", "grid_risk_snapshots"}:
            params["order"] = "created_at.desc" if table_name == "grid_events" else "timestamp.desc"
        rows = repo.select(table_name, params)
        return {"ok": True, "table": table_name, "rows": rows}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
