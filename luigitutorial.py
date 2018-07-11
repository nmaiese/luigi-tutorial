# Full Tutorial on  http://bionics.it/posts/luigi-tutorial
# An example of a simple "hello world" luigi task (that just prints "Hello world" in a new file), 
# complete with the code required to run the python file as a luigi script

import luigi

class HelloWorld(luigi.Task):
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget('helloworld.txt')
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('Hello World!\n')

if __name__ == '__main__':
    luigi.run()