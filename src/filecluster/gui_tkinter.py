#!/usr/bin/env python3
import tkinter as tk
from tkinter import filedialog, messagebox


def run_app():
    inbox = inbox_entry.get()
    lib = lib_entry.get()
    output = output_entry.get()
    # Replace with your clustering functionality call
    res = f"Inbox: {inbox}\nLibrary: {lib}\nOutput: {output}"
    messagebox.showinfo("Result", res)


def browse_folder(entry):
    folder = filedialog.askdirectory()
    if folder:
        entry.delete(0, tk.END)
        entry.insert(0, folder)


root = tk.Tk()
root.title("Media cluster by event")

# Create labels and entries
tk.Label(root, text="Inbox dir:").grid(row=0, column=0, sticky="e")
inbox_entry = tk.Entry(root, width=50)
inbox_entry.grid(row=0, column=1)
tk.Button(root, text="Browse", command=lambda: browse_folder(inbox_entry)).grid(
    row=0, column=2
)

tk.Label(root, text="Main library dir:").grid(row=1, column=0, sticky="e")
lib_entry = tk.Entry(root, width=50)
lib_entry.grid(row=1, column=1)
tk.Button(root, text="Browse", command=lambda: browse_folder(lib_entry)).grid(
    row=1, column=2
)

tk.Label(root, text="Output dir:").grid(row=2, column=0, sticky="e")
output_entry = tk.Entry(root, width=50)
output_entry.grid(row=2, column=1)
tk.Button(root, text="Browse", command=lambda: browse_folder(output_entry)).grid(
    row=2, column=2
)

# Run button
tk.Button(root, text="Run", command=run_app).grid(row=3, column=1, pady=10)

root.mainloop()
