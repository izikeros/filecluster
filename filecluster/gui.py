#!/usr/bin/env Python3
import PySimpleGUI as sg

# sg.ChangeLookAndFeel("GreenTan")

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
                        key="duplicates",
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
            "Inbox folder",
            size=(15, 1),
            auto_size_text=False,
            justification="right",
        ),
        sg.InputText("h:\incomming\inbox", key="inbox"),
        sg.FolderBrowse(),
    ],
    [
        sg.Text("Library 1", size=(15, 1), auto_size_text=False, justification="right"),
        sg.InputText("h:\zdjecia", key="lib_1"),
        sg.FolderBrowse(),
    ],
    [
        sg.Text("Library 2", size=(15, 1), auto_size_text=False, justification="right"),
        sg.InputText("h:\incomming\clustered", key="lib_2"),
        sg.FolderBrowse(),
    ],
    [sg.Submit(button_text="Run", tooltip="Click to start clustering"), sg.Cancel()],
]

window = sg.Window(
    "Everything bagel", layout, default_element_size=(40, 1), grab_anywhere=False
)

event, values = window.read()

window.close()

# parse values and event
sg.popup(
    "Title",
    "The results of the window.",
    'The button clicked was "{}"'.format(event),
    "The values are",
    values,
)
