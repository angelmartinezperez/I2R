import pickle
import tempfile

from jobs import ArgumentJob

if __name__ == "__main__":
    job = ArgumentJob("MyJob", "A test job", "1.0.0", "John Doe")
    print("Job created with the following info:")
    job.print_info()

    job.send()
