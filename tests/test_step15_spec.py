from __future__ import annotations

import pandas as pd

from probability_engine.research.step15_spec import (
    SPEC_VERSION,
    model_id,
    quality_grade,
    semantic_description,
    semantic_name,
    validate_manifest,
)


def test_model_id_is_stable_and_versioned():
    assert model_id("path_inside_70", "8H") == f"{SPEC_VERSION}__path_inside_70__8h"


def test_semantic_name_and_description_are_explicit():
    assert semantic_name("realized_over_range_width_ge_1", "12H") == "P_REALIZED_OVER_RANGE_WIDTH_GE_1_12H"
    assert "future 12H" in semantic_description("realized_over_range_width_ge_1", "12H")


def test_manifest_validation_rejects_duplicate_model_ids():
    manifest = {
        "models": [
            {
                "model_id": "x",
                "target": "path_inside_70",
                "horizon": "8H",
                "semantic_name": "P_PATH_INSIDE_70_8H",
                "semantic_description": "desc",
                "artifact_path": "a.pkl",
                "artifact_sha256": "abc",
                "probability_output": "float in [0, 1]",
            },
            {
                "model_id": "x",
                "target": "path_inside_70",
                "horizon": "12H",
                "semantic_name": "P_PATH_INSIDE_70_12H",
                "semantic_description": "desc",
                "artifact_path": "b.pkl",
                "artifact_sha256": "def",
                "probability_output": "float in [0, 1]",
            },
        ]
    }

    assert "duplicate model_id" in validate_manifest(manifest)


def test_quality_grade_prefers_probability_quality_and_robustness():
    assert quality_grade(0.10, 0.70, 0.04, True, 3) == "HIGH"
    assert quality_grade(-0.01, 0.80, 0.02, True, 3) == "RESEARCH_ONLY"


def test_range_breach_semantics_documents_complement():
    desc = semantic_description("range_breached", "8H")

    assert "1 - P_PATH_INSIDE_70_8H" in desc
