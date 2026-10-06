import sys
import tempfile
import pickle

from jobs import ArgumentJob

if __name__ == "__main__":
    # Print all arguments
    print("Arguments:", sys.argv[1])

    file = tempfile.NamedTemporaryFile()
    filename = file.name
    print("Temp file name:", filename)

    job = ArgumentJob("MyJob", "A test job", "1.0.0", "John Doe")
    print("Job created with the following info:")
    job.print_info()

    # Serialize the job object to a temporary file
    with open(filename, 'wb') as f:
        pickle.dump(job, f)

    # Deserialize the job object from the temporary file
    with open(filename, 'rb') as f:
        loaded_job = pickle.load(f)

    print("Loaded job from temp file with the following info:")
    loaded_job.print_info()
    print(f"Job class: {loaded_job.__class__.__name__}")
        