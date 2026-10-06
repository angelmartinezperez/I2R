
from jobs.job import Job

class ArgumentJob(Job):
    """Create a subjob."""

    def __init__(self, name, description, version, author):
        super().__init__()
        self.name = name
        self.description = description
        self.version = version
        self.author = author

    def print_info(self):
        print(f"Job Name: {self.name}")
        print(f"Description: {self.description}")
        print(f"Version: {self.version}")
        print(f"Author: {self.author}")

    def run(self):
        """
        Run the job.
        Intended to be called from the pyflink client (by the command flink run).
        This method should create and execute the environment.
        """
        self.print_info()