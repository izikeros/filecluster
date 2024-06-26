#!/usr/bin/env python3
"""Simple GUI to configure and run filecluster. EXPERIMENTAL."""
import PySimpleGUI as sg  # noqa: N813

from filecluster.file_cluster import main

# TODO: KS: 2020-12-28: read default values from configuration module

# ------ Menu Definition ------ #

menu_def = [
    ["File", ["Load configuration", "Save configuration", "Exit"]],
    ["Help", "About..."],
]

layout = [
    [sg.Menu(menu_def, tearoff=True)],
    [
        sg.Frame(
            layout=[
                [
                    sg.Checkbox(
                        "Do not import duplicates existing in the library",
                        default=True,
                        size=(50, 1),
                        key="drop_duplicates",
                    ),
                ],
                [
                    sg.Checkbox(
                        "Use existing clusters information",
                        default=True,
                        key="use_existing",
                    ),
                ],
                [
                    sg.Checkbox("Dry run", default=False, key="dry_run"),
                ],
                [
                    sg.Checkbox(
                        "Rebuild cluster info in libraries",
                        default=False,
                        key="force_deep_scan",
                    ),
                ],
                [sg.Text("Operations used to organize media files")],
                [
                    sg.Radio("Move", "RADIO1", default=True, size=(10, 1), key="move"),
                    sg.Radio("Copy", "RADIO1", key="copy"),
                ],
                [sg.Text("Max allowed gap between media in event [minutes]")],
                [sg.InputText("60", key="gap")],
            ],
            title="Options",
            relief=sg.RELIEF_SUNKEN,
            tooltip="Use these to set flags",
        )
    ],
    [sg.Text("_" * 80)],
    [sg.Text("Choose folders", size=(35, 1))],
    [
        sg.Text(
            "Inbox dir",
            size=(15, 1),
            auto_size_text=False,
            justification="right",
        ),
        sg.InputText("h:\\incomming\\inbox", key="inbox"),
        sg.FolderBrowse(),
    ],
    [
        sg.Text(
            "Main library dir",
            size=(15, 1),
            auto_size_text=False,
            justification="right",
        ),
        sg.InputText("h:\\zdjecia", key="lib_1"),
        sg.FolderBrowse(),
    ],
    [
        sg.Text(
            "Output dir",
            size=(15, 1),
            auto_size_text=False,
            justification="right",
        ),
        sg.InputText("h:\\incomming\\clustered", key="output"),
        sg.FolderBrowse(),
    ],
    [sg.Submit(button_text="Run", tooltip="Click to start clustering"), sg.Cancel()],
]

window = sg.Window(
    title="Media cluster by event.",
    layout=layout,
    default_element_size=(40, 1),
    grab_anywhere=False,
)

event, values = window.read()

window.close()

# parse values and event
sg.popup(
    "Title",
    "The results of the window.",
    f'The button clicked was "{event}"',
    "The values are",
    values,
)

main(
    inbox_dir=values["inbox"],
    output_dir=values["output"],
    watch_dir_list=[values["lib_1"]],
    development_mode=False,
    no_operation=values["dry_run"],
    copy_mode=values["copy"],
    force_deep_scan=values["force_deep_scan"],
    drop_duplicates=values["drop_duplicates"],
    use_existing_clusters=values["use_existing"],
)
