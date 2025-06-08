import luigi
from pipeline.extract import Extract
from pipeline.load import Load
from pipeline.transform import Transform
import os
from datetime import datetime

# Define output folder paths
OUTPUT_FOLDER = "./luigi-output"

class Extract(luigi.Task):
    def output(self):
        return luigi.LocalTarget(os.path.join(OUTPUT_FOLDER, 'extract_done.txt'))

    def run(self):
        Extract()
        with self.output().open('w') as f:
            f.write(f"Extract completed at {datetime.now()}.\n")

class Load(luigi.Task):
    def requires(self):
        return Extract()

    def output(self):
        return luigi.LocalTarget(os.path.join(OUTPUT_FOLDER, 'load_done.txt'))

    def run(self):
        Load()
        with self.output().open('w') as f:
            f.write(f"Load completed at {datetime.now()}.\n")

class Transform(luigi.Task):
    def requires(self):
        return Load()

    def output(self):
        return luigi.LocalTarget(os.path.join(OUTPUT_FOLDER, 'transform_done.txt'))

    def run(self):
        Transform()
        with self.output().open('w') as f:
            f.write(f"Transform completed at {datetime.now()}.\n")

if __name__ == "__main__":
    # Ensure the output folder exists
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    # Build the task
    luigi.build([Extract(),
                 Load(),
                 Transform()], local_scheduler=True)