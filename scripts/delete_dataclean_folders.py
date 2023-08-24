import os
import shutil

def delete_subfolders_with_prefix(path, prefix="data_"):
    """Delete all subfolders starting with the given prefix and their contents from the given path."""
    if not os.path.exists(path):
        print(f"Path not found: {path}")
        return

    for item in os.listdir(path):
        item_path = os.path.join(path, item)
        if os.path.isdir(item_path) and item.startswith(prefix):
            print('deleteing', item_path)
            shutil.rmtree(item_path)

# Usage:
# folder_path = "/path/to/your/folder"
folder_path = os.getcwd()
delete_subfolders_with_prefix(folder_path)

