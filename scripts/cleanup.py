import shutil
import os

def delete_directory(directory_path):
    """Delete a directory and its contents."""
    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)
        print(f"Deleted: {directory_path}")
    else:
        print(f"Directory does not exist: {directory_path}")

def main():
    directories_to_delete = [
        "/data/download",
        "/data/staging",
        "/data/staging.gdb"
    ]

    for directory in directories_to_delete:
        delete_directory(directory)

if __name__ == "__main__":
    main()
