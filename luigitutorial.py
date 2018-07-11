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

#  Let's try adding another task, NameSubstituter, that will take the file we created in our HelloWorld task, and replace "World" with some name.

class NameSubstituter(luigi.Task):
    name = luigi.Parameter()

    def requires(self): 
        return HelloWorld()
    def output(self): 
        return luigi.LocalTarget(self.input().path + '.name_' + self.name)
    def run(self): 
        with self.input().open() as infile, self.output().open('w') as outfile:
            text = infile.read()
            text = text.replace('World', self.name)
            outfile.write(text)



if __name__ == '__main__':
    luigi.run()