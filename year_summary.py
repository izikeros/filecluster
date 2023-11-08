import os
import random
import shutil
from pathlib import Path

from PIL import Image

# Set the path to the root directory containing the year directories
root_dir = Path("/Volumes/Multimedia/zdjecia")
year = 2022

# Set the number of photos to select from each year
num_photos_per_year = 10

# Set the maximum width for the resized photos
max_width = 1024

# Create a directory to store the selected photos
selected_dir = os.path.join(root_dir, f"{year}_year_summary")
if not os.path.exists(selected_dir):
    os.makedirs(selected_dir)

# Loop through each year directory
year_dir = root_dir / str(year)

# Create a list to store the selected photos for this year
selected_photos = []

# (only include photos with the following extensions)
# ".jpg", ".jpeg", ".png", ".gif", ".bmp"
# and iphone:
# ".heic", ".heif"
extensions = (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".heic", ".heif")

# Loop through each event directory in the year directory
for event_dir in os.listdir(os.path.join(root_dir, year_dir)):
    if not os.path.isdir(os.path.join(root_dir, year_dir, event_dir)):
        continue

    # Get a list of all the photos in the event directory
    photos = [
        filename
        for filename in os.listdir(os.path.join(root_dir, year_dir, event_dir))
        if filename.lower().endswith(extensions)
    ]

    # If there are no photos in the event directory, continue to the next directory
    if not photos:
        continue

    # Select a random photo from the event directory
    selected_photo = random.choice(photos)

    # If the selected photo has already been selected for this year, continue to the next directory
    if selected_photo in selected_photos:
        continue

    # Add the selected photo to the list of selected photos for this year
    selected_photos.append(selected_photo)

    # Copy the selected photo to the selected directory
    src_path = os.path.join(root_dir, year_dir, event_dir, selected_photo)
    dst_path = os.path.join(selected_dir, selected_photo)
    shutil.copyfile(src_path, dst_path)

    # if heic or heif, convert to jpg
    if selected_photo.lower().endswith((".heic", ".heif")):
        try:
            img = Image.open(dst_path)
            img = img.convert("RGB")
            img.save(dst_path, "JPEG")
        except:
            print(f"Could not convert {dst_path}")

    # Resize the selected photo
    try:
        img = Image.open(dst_path)
        w, h = img.size
        if w > max_width:
            new_h = int(h * max_width / w)
            img = img.resize((max_width, new_h))
            img.save(dst_path)
    except:
        print(f"Could not resize {dst_path}")

    # If we have selected the maximum number of photos for this year, break out of the loop
    if len(selected_photos) == num_photos_per_year:
        break
print("Done!")
