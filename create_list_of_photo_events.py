import os
import re
from pathlib import Path

D_NAME = "h:\\zdjecia"
lst = os.listdir(D_NAME)
dirs = [x for x in lst if os.path.isdir(str(Path(D_NAME) / x))]
year_dir = [x for x in dirs if re.match(r"\d\d\d\d", x)]

events = []
for y in year_dir:
    lst = os.listdir(str(Path(D_NAME) / y))
    y_events = [x for x in lst if os.path.isdir(str(Path(D_NAME) / y / x))]
    events.extend(y_events)


def correct_event(name: str):
    d = None
    e = None
    try:
        d, e = name.split("]_")
    except ValueError:
        print(f"Wrong name {name}")
        return None

    if d:
        d = d[1:]
        d_rep = d.replace("_", "-")
    else:
        print(f"Wrong date in name: {name}")
        return None

    if e:
        e_rep = e.replace("_", " ")
    else:
        print(f"Wrong event name for name: {name}")
        e_rep = "unlabelled"

    return f"{d_rep};{e_rep}"


events_corr = [correct_event(e) for e in events]
events_corr = [e + "\n" for e in events_corr if e is not None]

with open("photo_events.csv", "w") as f:
    f.writelines(events_corr)
