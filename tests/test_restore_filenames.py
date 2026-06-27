"""Tests for the restore-original-filenames feature.

Covers:
- strip_copy_suffix (pure string helper)
- resolve_destination_names (collision-aware mapping)
- build_file_operation_plan(restore_original_names=True) behaviour
"""

from pathlib import Path

import pandas as pd
import pytest

from filecluster.configuration import CopyMode
from filecluster.file_operations import (
    CopyOp,
    MoveOp,
    build_file_operation_plan,
    resolve_destination_names,
    strip_copy_suffix,
)


class TestStripCopySuffix:
    """Unit tests for the pure strip_copy_suffix helper."""

    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("IMG_0017-Kopiuj(2).HEIC", "IMG_0017.HEIC"),
            ("IMG_0017-Kopiuj(1).HEIC", "IMG_0017.HEIC"),
            ("IMG_0017-Kopiuj.HEIC", "IMG_0017.HEIC"),
            ("photo - Copy.jpg", "photo.jpg"),
            ("photo - Copy (3).jpg", "photo.jpg"),
            ("photo (1).jpg", "photo.jpg"),
            ("photo(1).jpg", "photo.jpg"),
            # No copy-suffix -> unchanged
            ("IMG_0017.HEIC", "IMG_0017.HEIC"),
            ("vacation.jpg", "vacation.jpg"),
        ],
    )
    def test_strip(self, name, expected):
        assert strip_copy_suffix(name) == expected

    def test_extension_preserved_case(self):
        """The original extension casing is preserved."""
        assert strip_copy_suffix("IMG_0017-Kopiuj(2).HEIC").endswith(".HEIC")

    def test_no_extension(self):
        """Files without an extension are handled."""
        assert strip_copy_suffix("README-Kopiuj(1)") == "README"


class TestResolveDestinationNames:
    """Tests for the collision-aware destination-name resolver."""

    @pytest.fixture()
    def inbox_df(self):
        # The user's real-world example, single target cluster.
        names = [
            "IMG_0015-Kopiuj(1).HEIC",
            "IMG_0015.HEIC",
            "IMG_0016-Kopiuj(1).HEIC",
            "IMG_0016-Kopiuj(2).HEIC",
            "IMG_0016.HEIC",
            "IMG_0017-Kopiuj(1).HEIC",
            "IMG_0017-Kopiuj(2).HEIC",
            "IMG_0017.HEIC",
        ]
        return pd.DataFrame(
            {"file_name": names, "target_path": ["new/cluster1"] * len(names)}
        )

    def test_restore_disabled_is_identity(self, inbox_df, tmp_path):
        mapping = resolve_destination_names(inbox_df, tmp_path, restore=False)
        for name in inbox_df["file_name"]:
            assert mapping[name] == name

    def test_true_originals_keep_their_name(self, inbox_df, tmp_path):
        mapping = resolve_destination_names(inbox_df, tmp_path, restore=True)
        assert mapping["IMG_0015.HEIC"] == "IMG_0015.HEIC"
        assert mapping["IMG_0016.HEIC"] == "IMG_0016.HEIC"
        assert mapping["IMG_0017.HEIC"] == "IMG_0017.HEIC"

    def test_copies_keep_suffix_when_original_present(self, inbox_df, tmp_path):
        """Since the un-suffixed original exists, copies must NOT be renamed."""
        mapping = resolve_destination_names(inbox_df, tmp_path, restore=True)
        assert mapping["IMG_0017-Kopiuj(1).HEIC"] == "IMG_0017-Kopiuj(1).HEIC"
        assert mapping["IMG_0017-Kopiuj(2).HEIC"] == "IMG_0017-Kopiuj(2).HEIC"

    def test_no_destination_collisions(self, inbox_df, tmp_path):
        """All resolved destination names within a cluster are unique."""
        mapping = resolve_destination_names(inbox_df, tmp_path, restore=True)
        dests = list(mapping.values())
        assert len(dests) == len(set(dests))

    def test_single_copy_no_original_is_promoted(self, tmp_path):
        """A lone copy with no un-suffixed original gets de-suffixed."""
        df = pd.DataFrame(
            {
                "file_name": ["IMG_0099-Kopiuj(1).HEIC"],
                "target_path": ["new/cluster1"],
            }
        )
        mapping = resolve_destination_names(df, tmp_path, restore=True)
        assert mapping["IMG_0099-Kopiuj(1).HEIC"] == "IMG_0099.HEIC"

    def test_two_copies_first_wins(self, tmp_path):
        """With two copies and no original, only one claims the stripped name."""
        df = pd.DataFrame(
            {
                "file_name": [
                    "IMG_0099-Kopiuj(1).HEIC",
                    "IMG_0099-Kopiuj(2).HEIC",
                ],
                "target_path": ["new/cluster1", "new/cluster1"],
            }
        )
        mapping = resolve_destination_names(df, tmp_path, restore=True)
        dests = sorted(mapping.values())
        assert "IMG_0099.HEIC" in dests
        # the other one keeps its suffixed name
        assert "IMG_0099-Kopiuj(2).HEIC" in dests

    def test_collision_with_existing_file_on_disk(self, tmp_path):
        """If the de-suffixed name already exists on disk, keep the suffix."""
        cluster_dir = tmp_path / "new" / "cluster1"
        cluster_dir.mkdir(parents=True)
        (cluster_dir / "IMG_0099.HEIC").write_text("existing")

        df = pd.DataFrame(
            {
                "file_name": ["IMG_0099-Kopiuj(1).HEIC"],
                "target_path": ["new/cluster1"],
            }
        )
        mapping = resolve_destination_names(df, tmp_path, restore=True)
        assert mapping["IMG_0099-Kopiuj(1).HEIC"] == "IMG_0099-Kopiuj(1).HEIC"


class TestBuildPlanWithRestore:
    """build_file_operation_plan honours restore_original_names."""

    @pytest.fixture()
    def inbox_df(self):
        return pd.DataFrame(
            {
                "file_name": ["IMG_1.HEIC", "IMG_2-Kopiuj(1).HEIC"],
                "target_path": ["new/c1", "new/c1"],
            }
        )

    def test_default_keeps_original_destination_names(self, inbox_df):
        """Without the flag, dst basename equals the inbox file name."""
        plan = build_file_operation_plan(
            inbox_media_df=inbox_df,
            in_dir=Path("/inbox"),
            out_dir=Path("/out"),
            mode=CopyMode.MOVE,
        )
        dsts = {op.dst.name for op in plan.ops if isinstance(op, MoveOp)}
        assert dsts == {"IMG_1.HEIC", "IMG_2-Kopiuj(1).HEIC"}

    def test_restore_strips_suffix_in_destination(self, inbox_df):
        """With the flag, the copy is renamed but src stays the inbox name."""
        plan = build_file_operation_plan(
            inbox_media_df=inbox_df,
            in_dir=Path("/inbox"),
            out_dir=Path("/out"),
            mode=CopyMode.MOVE,
            restore_original_names=True,
        )
        moves = {op.src.name: op.dst.name for op in plan.ops if isinstance(op, MoveOp)}
        assert moves["IMG_1.HEIC"] == "IMG_1.HEIC"
        # source unchanged, destination de-suffixed
        assert moves["IMG_2-Kopiuj(1).HEIC"] == "IMG_2.HEIC"

    def test_restore_works_in_copy_mode(self, inbox_df):
        plan = build_file_operation_plan(
            inbox_media_df=inbox_df,
            in_dir=Path("/inbox"),
            out_dir=Path("/out"),
            mode=CopyMode.COPY,
            restore_original_names=True,
        )
        copies = {op.src.name: op.dst.name for op in plan.ops if isinstance(op, CopyOp)}
        assert copies["IMG_2-Kopiuj(1).HEIC"] == "IMG_2.HEIC"

    def test_nop_mode_unaffected(self, inbox_df):
        plan = build_file_operation_plan(
            inbox_media_df=inbox_df,
            in_dir=Path("/inbox"),
            out_dir=Path("/out"),
            mode=CopyMode.NOP,
            restore_original_names=True,
        )
        assert plan.n_skips == 2
        assert plan.n_moves == 0
