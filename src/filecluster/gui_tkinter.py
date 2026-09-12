#!/usr/bin/env python3
"""Experimental Tkinter launcher.

This prototype is intentionally isolated from the CLI and package imports.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import filedialog


def main() -> None:
    """Launch the placeholder Tkinter prototype."""
    root = tk.Tk()
    root.title("Filecluster (experimental)")

    def browse(entry: tk.Entry) -> None:
        if folder := filedialog.askdirectory():
            entry.delete(0, tk.END)
            entry.insert(0, folder)

    for row, label in enumerate(
        ("Inbox directory:", "Main library directory:", "Output directory:")
    ):
        tk.Label(root, text=label).grid(row=row, column=0, sticky="e")
        entry = tk.Entry(root, width=50)
        entry.grid(row=row, column=1)
        tk.Button(root, text="Browse", command=lambda e=entry: browse(e)).grid(
            row=row, column=2
        )

    tk.Label(
        root, text="This prototype is not connected to the clustering workflow."
    ).grid(row=3, column=0, columnspan=3, pady=10)
    tk.Button(root, text="Close", command=root.destroy).grid(row=4, column=1, pady=10)
    root.mainloop()


if __name__ == "__main__":  # pragma: no cover - manual experimental entry point
    main()
