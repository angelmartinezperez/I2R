
import tempfile
import pickle
import os
import subprocess
from pathlib import Path

# imports needed to be able to find the folder of the package and entrypoint name.
import jobs
import jobs.entrypoint

class Job:
    """Base (abstract) class for flink jobs."""

    def send(self):
        """
        Send the job to the flink cluster.
        This is the dev implementation. It sends the 'jobs' package by finding the folder from which it was imported.
        In production, this would be done with a requirements.txt file (that is propagated to avoid version issues) and a private package registry.
        """
        flink_home = os.environ["FLINK_HOME"]
        flink_executable = os.path.join(flink_home, "bin", "flink")

        entrypoint_module = jobs.entrypoint.__name__
        # Due to the way it is parsed, it is important that the path passed to -pyfs is absolute.
        package_parent_folder = Path(jobs.__file__).resolve().parent.parent
        with self._save_to_temp_file() as pickle_file:
            result = subprocess.run(
                [
                    flink_executable,
                    "run",
                    "--pyFiles", package_parent_folder,
                    "--pyModule", entrypoint_module,
                    "--detached", # Only wait for the job to be created, not finished.
                    pickle_file.name
                ]
            )

        if result.returncode != 0:
            raise RuntimeError(
                f"Flink job failed with exit code {result.returncode}"
            )

    def run(self):
        """
        Run the job.
        Intended to be called from the pyflink client (by the command flink run).
        This method should create and execute the environment.
        """
        raise NotImplementedError("This method should be implemented by the Job subclasses.")

    def _save_to_temp_file(self):
        """
        Save the job to a temporary file.
        This is used to send the job to the flink cluster.
        """
        file = tempfile.NamedTemporaryFile()
        with open(file.name, 'wb') as f:
            pickle.dump(self, f)
        return file