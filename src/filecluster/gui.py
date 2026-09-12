#!/usr/bin/env python3
"""Experimental PySimpleGUI launcher for the clustering workflow."""

from __future__ import annotations

from pathlib import Path


def main() -> None:
    """Launch the experimental GUI without affecting normal package imports."""
    try:
        import PySimpleGUI as sg  # noqa: N813
    except ImportError as exc:
        raise RuntimeError(
            "The experimental GUI needs PySimpleGUI. Install filecluster[gui]."
        ) from exc

    from filecluster.file_cluster import ClusterRequest, cluster

    layout = [
        [
            sg.Frame(
                "Options",
                [
                    [
                        sg.Checkbox(
                            "Separate duplicates already in the library",
                            default=True,
                            key="duplicates",
                        )
                    ],
                    [
                        sg.Checkbox(
                            "Assign files to existing events",
                            default=True,
                            key="existing",
                        )
                    ],
                    [sg.Checkbox("Dry run", default=True, key="dry_run")],
                    [
                        sg.Checkbox(
                            "Rebuild library metadata",
                            default=False,
                            key="force_deep_scan",
                        )
                    ],
                    [
                        sg.Radio("Move", "operation", default=True, key="move"),
                        sg.Radio("Copy", "operation", key="copy"),
                    ],
                ],
            )
        ],
        [
            sg.Text("Inbox directory", size=(18, 1)),
            sg.Input(key="inbox"),
            sg.FolderBrowse(),
        ],
        [
            sg.Text("Main library directory", size=(18, 1)),
            sg.Input(key="library"),
            sg.FolderBrowse(),
        ],
        [
            sg.Text("Output directory", size=(18, 1)),
            sg.Input(key="output"),
            sg.FolderBrowse(),
        ],
        [sg.Button("Run"), sg.Button("Cancel")],
    ]
    window = sg.Window("Filecluster (experimental)", layout)
    event, values = window.read()
    window.close()
    if event != "Run":
        return

    request = ClusterRequest(
        inbox=Path(values["inbox"]),
        output=Path(values["output"]),
        watch_dirs=(Path(values["library"]),) if values["library"] else (),
        dry_run=values["dry_run"],
        copy_files=values["copy"],
        force_deep_scan=values["force_deep_scan"],
        separate_duplicates=values["duplicates"],
        assign_existing_clusters=values["existing"],
    )
    run = cluster(request)
    sg.popup(
        "Filecluster complete",
        f"{run.files_read} files processed",
        f"{len(run.new_folder_names)} new event folders",
        "Dry run; no files changed." if run.config.mode.name == "NOP" else "Done.",
    )


if __name__ == "__main__":  # pragma: no cover - manual experimental entry point
    main()
