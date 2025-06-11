import multiprocessing
import subprocess
import os
import zipfile
import time

def run_part(part_number):
    print(f"🚀 Starting part {part_number}")
    result = subprocess.run(["python", "process_part.py", str(part_number)], capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"✅ Part {part_number} completed")
    else:
        print(f"❌ Part {part_number} failed\n{result.stderr}")

def zip_outputs():
    zip_filename = "all_outputs.zip"
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for i in range(1, 6):
            output_file = f"output_part_{i}.parquet"
            if os.path.exists(output_file):
                zipf.write(output_file)
                print(f"🗜️ Added {output_file} to {zip_filename}")
            else:
                print(f"⚠️ {output_file} not found, skipping...")
    print(f"\n🎉 Zipping complete. Download `all_outputs.zip` from Railway")

def main():
    parts = [1, 2, 3, 4, 5]
    processes = []

    for part in parts:
        p = multiprocessing.Process(target=run_part, args=(part,))
        p.start()
        processes.append(p)
        time.sleep(2)  # small stagger to avoid GDrive + API race

    for p in processes:
        p.join()

    zip_outputs()

if __name__ == "__main__":
    main()
