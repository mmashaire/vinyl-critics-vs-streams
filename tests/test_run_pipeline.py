from __future__ import annotations

import pytest

from scripts import run_pipeline


def test_filter_steps_supports_start_stop_and_skip() -> None:
    selected = run_pipeline.filter_steps(
        run_pipeline.PIPELINE,
        start="stage_reviews",
        stop="load_dim_artist",
        skip=["load_reviews_and_bridge"],
    )

    assert [name for name, _ in selected] == [
        "stage_reviews",
        "make_review_artists_bridge",
        "build_artist_universe",
        "clean_spotify_youtube",
        "match_artists",
        "load_dim_artist",
    ]


def test_filter_steps_rejects_unknown_start_or_stop() -> None:
    with pytest.raises(ValueError, match="Unknown --from"):
        run_pipeline.filter_steps(
            run_pipeline.PIPELINE,
            start="not-a-real-step",
            stop=None,
            skip=[],
        )

    with pytest.raises(ValueError, match="Unknown --until"):
        run_pipeline.filter_steps(
            run_pipeline.PIPELINE,
            start=None,
            stop="also-not-real",
            skip=[],
        )


def test_main_list_outputs_pipeline_summary(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["run_pipeline.py", "--list"],
    )

    exit_code = run_pipeline.main()

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Pipeline steps:" in captured.out
    assert "validate_dw" in captured.out
