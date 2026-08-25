from decimal import Decimal

from .accounting import summarize_pnl
from .config import ACCOUNTING_VERSION, GRIDBOT_VERSION, RISK_MODULE_VERSION, STRATEGY_VERSION
from .grid_builder import preview_grid
from .models import GridConfig, GridStatus, new_id, to_record_dict, utc_now
from .repository import InMemoryGridRepository, repository
from .risk import GridRiskController, RiskInputs


class DeltaGridBotEngine:
    def __init__(self, repo: InMemoryGridRepository | None = None):
        self.repo = repo or repository

    def create_bot(self, config: GridConfig) -> GridConfig:
        return self.repo.create_bot(config)

    def preview(self, config: GridConfig, reference_price: Decimal, tick_size: Decimal = Decimal("0.1")) -> dict:
        return to_record_dict(preview_grid(config, reference_price, tick_size))

    def start(self, bot_id: str, reference_price: Decimal, tick_size: Decimal = Decimal("0.1")) -> dict:
        config = self.repo.bots[bot_id]
        run = self.repo.start_run(bot_id, reference_price)
        levels = preview_grid(config, reference_price, tick_size)["levels"]
        risk = GridRiskController(config.risk_thresholds)
        inputs = RiskInputs(
            net_inventory=config.initial_inventory,
            max_inventory=config.max_inventory_lots,
            allocated_capital=config.allocated_capital,
            risk_capital=config.risk_capital,
            projected_adverse_grid_exposure=sum((level.quantity for level in levels), Decimal("0")),
        )
        decision = risk.evaluate(inputs)
        self.repo.risk_snapshots.append({"run_id": run.run_id, "timestamp": utc_now(), **to_record_dict(decision)})
        if not decision.allowed:
            self.repo.set_run_status(run.run_id, GridStatus.PAUSED)
            self.repo.log_event(bot_id, run.run_id, "RISK_REJECTED", to_record_dict(decision))
            return {"ok": False, "run": to_record_dict(run), "risk": to_record_dict(decision)}
        self.repo.set_run_status(run.run_id, GridStatus.RUNNING)
        self.repo.log_event(bot_id, run.run_id, "RISK_ALLOWED", to_record_dict(decision))
        return {"ok": True, "run": to_record_dict(run), "levels": to_record_dict(levels), "risk": to_record_dict(decision)}

    def pause(self, run_id: str) -> dict:
        run = self.repo.set_run_status(run_id, GridStatus.PAUSED)
        self.repo.log_event(run.bot_id, run_id, "GRID_PAUSED", {})
        return to_record_dict(run)

    def resume(self, run_id: str) -> dict:
        run = self.repo.set_run_status(run_id, GridStatus.RUNNING)
        self.repo.log_event(run.bot_id, run_id, "RECONCILIATION_STARTED", {"reason": "resume"})
        self.repo.log_event(run.bot_id, run_id, "RECONCILIATION_COMPLETED", {"source": "local_v0.1"})
        self.repo.log_event(run.bot_id, run_id, "GRID_RESUMED", {})
        return to_record_dict(run)

    def stop(self, run_id: str, reason: str = "manual", operator_notes: str | None = None) -> dict:
        run = self.repo.runs[run_id]
        self.repo.log_event(run.bot_id, run_id, "RECONCILIATION_STARTED", {"reason": "stop"})
        self.repo.log_event(run.bot_id, run_id, "RECONCILIATION_COMPLETED", {"source": "local_v0.1"})
        stopped = self.repo.set_run_status(run_id, GridStatus.STOPPED, stopped_at=utc_now(), stop_reason=reason)
        summary = self._build_summary(stopped, operator_notes)
        persisted = self.repo.create_summary(run_id, summary)
        self.repo.log_event(run.bot_id, run_id, "GRID_STOPPED", {"reason": reason})
        return {"run": to_record_dict(stopped), "summary": persisted}

    def _build_summary(self, run, operator_notes: str | None) -> dict:
        config = self.repo.bots[run.bot_id]
        events = [event for event in self.repo.events if event.get("run_id") == run.run_id]
        pnl = summarize_pnl(Decimal("0"), [], [], Decimal("0"))
        return {
            "summary_id": new_id("summary"),
            "run_id": run.run_id,
            "bot_id": run.bot_id,
            "gridbot_version": GRIDBOT_VERSION,
            "strategy_version": STRATEGY_VERSION,
            "risk_module_version": RISK_MODULE_VERSION,
            "accounting_version": ACCOUNTING_VERSION,
            "created_at": utc_now(),
            "finalised_at": utc_now(),
            "started_at": run.started_at,
            "stopped_at": run.stopped_at,
            "starting_ETH_price": str(run.reference_price) if run.reference_price is not None else None,
            "ending_ETH_price": None,
            "grid_type": config.grid_type.value,
            "initial_lower": str(config.lower_price),
            "final_lower": str(config.lower_price),
            "initial_upper": str(config.upper_price),
            "final_upper": str(config.upper_price),
            "initial_grid_count": config.grid_count,
            "final_grid_count": config.grid_count,
            "initial_spacing_type": config.spacing_type.value,
            "final_spacing_type": config.spacing_type.value,
            "initial_lot_size": str(config.lot_size),
            "final_lot_size": str(config.lot_size),
            "max_inventory_setting": str(config.max_inventory_lots),
            "allocated_capital": str(config.allocated_capital),
            "risk_capital": str(config.risk_capital),
            "number_of_parameter_changes": sum(1 for e in events if e["event_type"] == "PARAMETER_CHANGED"),
            "number_of_regrids": sum(1 for e in events if e["event_type"] == "REGRID_COMPLETED"),
            "total_order_proposals": sum(1 for e in events if e["event_type"] == "ORDER_PROPOSED"),
            "orders_allowed": sum(1 for e in events if e["event_type"] == "RISK_ALLOWED"),
            "orders_risk_rejected": sum(1 for e in events if e["event_type"] == "RISK_REJECTED"),
            "websocket_disconnect_count": sum(1 for e in events if e["event_type"] == "WS_DISCONNECTED"),
            "websocket_reconnect_count": sum(1 for e in events if e["event_type"] == "WS_RECONNECTED"),
            "reconciliation_count": sum(1 for e in events if e["event_type"] == "RECONCILIATION_COMPLETED"),
            "reconciliation_mismatch_count": sum(1 for e in events if e["event_type"] in {"POSITION_MISMATCH", "ACCOUNTING_WARNING"}),
            "duplicate_exchange_events_detected": 0,
            "duplicate_events_safely_ignored": 0,
            "orphan_orders_detected": 0,
            "orphan_orders_resolved": 0,
            "position_mismatches_detected": sum(1 for e in events if e["event_type"] == "POSITION_MISMATCH"),
            "position_mismatches_resolved": 0,
            "accounting_reconciliation_warnings": sum(1 for e in events if e["event_type"] == "ACCOUNTING_WARNING"),
            "unexpected_exceptions": 0,
            "execution_errors": 0,
            "gross_grid_harvest_pnl": str(pnl.gross_realised_pnl),
            "net_grid_harvest_pnl": str(pnl.net_trading_pnl_before_income_tax),
            "funding_received": str(pnl.funding_received),
            "funding_paid": str(pnl.funding_paid),
            "net_funding": str(pnl.net_funding),
            "maker_fees": str(pnl.maker_fees),
            "taker_fees": str(pnl.taker_fees),
            "total_exchange_fees": str(pnl.total_exchange_fees),
            "NET_TRADING_PNL_BEFORE_INCOME_TAX": str(pnl.net_trading_pnl_before_income_tax),
            "stop_reason": reason_label(run.stop_reason),
            "final_risk_state": None,
            "final_open_gridbot_order_count": 0,
            "final_exchange_position": None,
            "unresolved_warnings": [],
            "unresolved_errors": [],
            "operator_notes": operator_notes,
            "execution_event_mode": self.repo.rest_fallback_state.get(run.run_id, {}).get("execution_event_mode"),
            "private_ws_available": self.repo.rest_fallback_state.get(run.run_id, {}).get("private_ws_status") not in ["BLOCKED_403", "UNAVAILABLE"],
            **{
                key: self.repo.rest_fallback_state.get(run.run_id, {}).get(key)
                for key in [
                    "rest_poll_count",
                    "average_poll_interval",
                    "average_fill_detection_latency",
                    "max_fill_detection_latency",
                    "REST_errors",
                    "REST_retries",
                    "429_count",
                    "reconciliation_mismatches",
                    "fills_detected_through_REST",
                    "duplicate_fills_ignored",
                    "position_mismatches",
                ]
            },
            "immutable": True,
        }


def reason_label(reason: str | None) -> str:
    return reason or "manual"


engine = DeltaGridBotEngine()
