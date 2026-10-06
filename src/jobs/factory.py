import pickle
import sys

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else None
    if path is None:
        print("Please provide a path to the job file.")
        sys.exit(1)

    # Deserialize the job object from the temporary file
    with open(path, 'rb') as f:
        job = pickle.load(f)

    job.run()