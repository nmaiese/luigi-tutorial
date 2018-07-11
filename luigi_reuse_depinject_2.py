# An other, more dynamic, way

import luigi

class TaskA(luigi.Task):
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget('task_a')
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('foo')

class TaskB(luigi.Task):
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget('task_b')
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('bar')

class TaskC(luigi.Task):
    upstream_task = luigi.Parameter(default=TaskA()) # <-- Notice how we set the upstream dependency as a luigi task!
    def requires(self):
        return self.upstream_task # <-- Notice this dependency!
    def output(self):
        return luigi.LocalTarget(self.input().path + '.task_c')
    def run(self):
        with self.input().open() as infile, self.output().open('w') as outfile:
            for line in infile:
                outfile.write(line)

# Let's create a workflow task "MyWorkflow", that requires TaskC, but with a 
# different upstream dependency (TaskB) instead of the default TaskA
class MyWorkflow(luigi.Task):
    def requires(self):
        return TaskC(
          upstream_task=TaskB() # <-- Notice how we switched the dependency in TaskC!
        )
    def output(self):
        return self.input()

if __name__ == '__main__':
    luigi.run()