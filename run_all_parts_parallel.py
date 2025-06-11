import subprocess

def run_part(part_number):
    print(f"\n🚀 Starting part {part_number}...\n")
    result = subprocess.run(["python", "process_part.py", str(part_number)], capture_output=True, text=True)

    if result.returncode == 0:
        print(f"✅ Part {part_number} completed successfully.")
    else:
        print(f"❌ Part {part_number} failed with error:\n{result.stderr}")

def main():
    for part in [1, 2, 3, 4, 5]:
        run_part(part)

if __name__ == "__main__":
    main()
