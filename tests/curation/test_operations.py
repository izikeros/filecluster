"""Tests for the file operation plan and its execution."""

import pytest

from filecluster.curation.exceptions import UnsafeRelativePathError
from filecluster.curation.operations import (
    OperationMode,
    OperationStatus,
    build_operation_plan,
    execute_plan,
    safe_relative_path,
)
from filecluster.curation.types import CurationDecision, CurationResult

from .conftest import make_item, write_photo


def result_for(path, decision=CurationDecision.KEEP, relative_path=None):
    return CurationResult(
        item=make_item(path, relative_path=relative_path),
        decision=decision,
        confidence=0.9,
        scores={},
        labels=(),
        reasons=(),
        stage_trace=(),
        pipeline_version="curation-1",
    )


class TestSafePaths:
    def test_accepts_a_nested_relative_path(self):
        assert safe_relative_path("2024/holiday/IMG_1.jpg").name == "IMG_1.jpg"

    @pytest.mark.parametrize("bad", ["/etc/passwd", "../outside.jpg", "a/../../b.jpg"])
    def test_refuses_paths_that_escape(self, bad):
        with pytest.raises(UnsafeRelativePathError):
            safe_relative_path(bad)


class TestPlan:
    def test_destinations_follow_the_decision_folders(self, tmp_path, out_dir):
        results = [
            result_for(write_photo(tmp_path / "a.jpg"), CurationDecision.KEEP),
            result_for(write_photo(tmp_path / "b.jpg"), CurationDecision.REVIEW),
            result_for(write_photo(tmp_path / "c.jpg"), CurationDecision.REJECT),
        ]

        plan = build_operation_plan(results, out_dir, OperationMode.COPY)

        assert [str(op.dst.relative_to(out_dir)) for op in plan.ops] == [
            "keep/a.jpg",
            "review/b.jpg",
            "reject/c.jpg",
        ]

    def test_nested_relative_paths_are_preserved(self, tmp_path, out_dir):
        path = write_photo(tmp_path / "2024" / "trip" / "a.jpg")
        results = [result_for(path, relative_path="2024/trip/a.jpg")]

        plan = build_operation_plan(results, out_dir)

        assert plan.ops[0].dst == out_dir / "keep" / "2024" / "trip" / "a.jpg"

    def test_an_existing_file_is_never_overwritten(self, tmp_path, out_dir):
        (out_dir / "keep").mkdir()
        (out_dir / "keep" / "a.jpg").write_bytes(b"existing")
        results = [result_for(write_photo(tmp_path / "a.jpg"))]

        plan = build_operation_plan(results, out_dir)

        assert plan.ops[0].dst.name == "a (1).jpg"
        assert plan.n_renamed == 1

    def test_two_files_from_one_run_cannot_collide(self, tmp_path, out_dir):
        results = [
            result_for(write_photo(tmp_path / "one" / "a.jpg"), relative_path="a.jpg"),
            result_for(write_photo(tmp_path / "two" / "a.jpg"), relative_path="a.jpg"),
        ]

        plan = build_operation_plan(results, out_dir)

        assert len({op.dst for op in plan.ops}) == 2

    def test_the_plan_is_deterministic(self, tmp_path, out_dir):
        results = [
            result_for(write_photo(tmp_path / "b.jpg")),
            result_for(write_photo(tmp_path / "a.jpg")),
        ]

        first = build_operation_plan(results, out_dir)
        second = build_operation_plan(list(reversed(results)), out_dir)

        assert [op.dst for op in first.ops] == [op.dst for op in second.ops]

    def test_a_dry_run_resolves_the_same_names_as_a_real_run(self, tmp_path, out_dir):
        """The preview is only useful if it is the truth."""
        (out_dir / "keep").mkdir()
        (out_dir / "keep" / "a.jpg").write_bytes(b"existing")
        results = [result_for(write_photo(tmp_path / "a.jpg"))]

        preview = build_operation_plan(results, out_dir, OperationMode.SKIP)
        real = build_operation_plan(results, out_dir, OperationMode.COPY)

        assert [op.dst for op in preview.ops] == [op.dst for op in real.ops]

    def test_a_dry_run_writes_nothing(self, tmp_path, out_dir):
        results = [result_for(write_photo(tmp_path / "a.jpg"))]
        plan = build_operation_plan(results, out_dir, OperationMode.SKIP)

        execute_plan(plan)

        assert list(out_dir.rglob("*")) == []

    def test_counts_are_grouped_by_decision(self, tmp_path, out_dir):
        results = [
            result_for(write_photo(tmp_path / "a.jpg"), CurationDecision.KEEP),
            result_for(write_photo(tmp_path / "b.jpg"), CurationDecision.KEEP),
            result_for(write_photo(tmp_path / "c.jpg"), CurationDecision.REJECT),
        ]

        plan = build_operation_plan(results, out_dir)

        assert plan.counts_by_decision() == {"keep": 2, "review": 0, "reject": 1}

    def test_preview_rows_describe_every_file(self, tmp_path, out_dir):
        results = [result_for(write_photo(tmp_path / "a.jpg"))]

        rows = build_operation_plan(results, out_dir).preview()

        assert rows == [(str(out_dir / "keep"), "a.jpg", "a.jpg")]


class TestExecution:
    def test_copy_leaves_the_source_in_place(self, tmp_path, out_dir):
        source = write_photo(tmp_path / "a.jpg")
        plan = build_operation_plan([result_for(source)], out_dir, OperationMode.COPY)

        execute_plan(plan)

        assert source.exists()
        assert (out_dir / "keep" / "a.jpg").exists()
        assert plan.status_for(source) is OperationStatus.COMPLETED

    def test_move_removes_the_source(self, tmp_path, out_dir):
        source = write_photo(tmp_path / "a.jpg")
        plan = build_operation_plan([result_for(source)], out_dir, OperationMode.MOVE)

        execute_plan(plan)

        assert not source.exists()
        assert (out_dir / "keep" / "a.jpg").exists()

    def test_move_does_not_clobber_an_existing_file(self, tmp_path, out_dir):
        (out_dir / "keep").mkdir()
        existing = out_dir / "keep" / "a.jpg"
        existing.write_bytes(b"existing")
        plan = build_operation_plan(
            [result_for(write_photo(tmp_path / "a.jpg"))], out_dir, OperationMode.MOVE
        )

        execute_plan(plan)

        assert existing.read_bytes() == b"existing"
        assert (out_dir / "keep" / "a (1).jpg").exists()

    @pytest.mark.parametrize("mode", [OperationMode.COPY, OperationMode.MOVE])
    def test_write_time_collision_never_overwrites(self, tmp_path, out_dir, mode):
        source = write_photo(tmp_path / "a.jpg")
        plan = build_operation_plan([result_for(source)], out_dir, mode)
        destination = plan.ops[0].dst
        destination.parent.mkdir(parents=True)
        destination.write_bytes(b"existing")

        execute_plan(plan)

        assert destination.read_bytes() == b"existing"
        assert (destination.parent / "a (1).jpg").exists()

    def test_a_failing_file_is_recorded_and_the_rest_continue(self, tmp_path, out_dir):
        good = write_photo(tmp_path / "a.jpg")
        missing = tmp_path / "gone.jpg"
        plan = build_operation_plan(
            [result_for(good), result_for(missing)], out_dir, OperationMode.COPY
        )

        execute_plan(plan)

        assert plan.status_for(good) is OperationStatus.COMPLETED
        assert plan.status_for(missing) is OperationStatus.FAILED
        assert plan.n_completed == 1
        assert plan.n_failed == 1

    def test_destinations_are_queryable(self, tmp_path, out_dir):
        source = write_photo(tmp_path / "a.jpg")
        plan = build_operation_plan([result_for(source)], out_dir)

        assert plan.destination_for(source) == out_dir / "keep" / "a.jpg"
        assert plan.destination_for(tmp_path / "absent.jpg") is None
