import shutil
import os

def delete_directory_contents(directory_path):
    """Delete contents of a directory without removing the directory itself."""
    if os.path.exists(directory_path):
        # Remove all contents
        for item in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
            else:
                os.remove(item_path)
        print(f"Cleared contents of: {directory_path}")
    else:
        print(f"Directory does not exist: {directory_path}")

def delete_directory(directory_path):
    """Delete a directory and its contents."""
    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)
        print(f"Deleted: {directory_path}")
    else:
        print(f"Directory does not exist: {directory_path}")

def main():
    # Define paths relative to the script's working directory
    clear_contents = [
        os.path.join("data", "downloads"),  # Note: you mentioned downloads (plural)
        os.path.join("data", "staging")
    ]
    
    delete_completely = [
        os.path.join("data", "staging.gdb")
    ]

    # Clear contents but keep directories
    for directory in clear_contents:
        delete_directory_contents(directory)
    
    # Delete directories completely
    for directory in delete_completely:
        delete_directory(directory)

if __name__ == "__main__":
    main()