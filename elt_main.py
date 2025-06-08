import luigi
from pipeline.extract import Extract
from pipeline.load import Load
from pipeline.transform import Transform

class Extract(luigi.Task):
    def run(self):
        Extract()

class Load(luigi.Task):
    def requires(self):
        return Extract()

    def run(self):
        Load()

class Transform(luigi.Task):
    def requires(self):
        return Load()

    def run(self):
        Transform()

if __name__ == "__main__":
    # Build the task
    luigi.build([Extract(),
                 Load(),
                 Transform()], local_scheduler=True)
