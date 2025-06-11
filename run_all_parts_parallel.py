import multiprocessing
import subprocess

def run_part(part_number):
    print(f"Starting part {part_number}")
    subprocess.run(["python", "process_part.py", str(part_number)])

def main():
    parts = [1, 2, 3, 4, 5]
    processes = []

    for part in parts:
        p = multiprocessing.Process(target=run_part, args=(part,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    print("✅ All parts completed.")

if __name__ == "__main__":
    main()
