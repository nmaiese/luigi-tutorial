# Full Tutorial on  http://bionics.it/posts/luigi-tutorial
# An example of a simple "hello world" luigi task (that just prints "Hello world" in a new file), 
# complete with the code required to run the python file as a luigi script

import luigi
import time

class HelloWorld(luigi.Task):
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget('helloworld.txt')
    def run(self):
        time.sleep(15)
        with self.output().open('w') as outfile:
            outfile.write('Hello World!\n')
        time.sleep(15)

#  Let's try adding another task, NameSubstituter, that will take the file we created in our HelloWorld task, and replace "World" with some name.

class NameSubstituter(luigi.Task):
    name = luigi.Parameter()

    def requires(self): 
        return HelloWorld()
    def output(self): 
        return luigi.LocalTarget(self.input().path + '.name_' + self.name)
    def run(self): 
        time.sleep(15)
        with self.input().open() as infile, self.output().open('w') as outfile:
            text = infile.read()
            text = text.replace('World', self.name)
            outfile.write(text)
        time.sleep(15)


# Visualizing running workflows (optional)
# In order to see what's happening before the workflow is finished, 
# we need to add a little sleep to the tasks, since they are running so fast. 
# So, let's add a sleep of 15 seconds before and after the main chunk of work in each of the tasks



if __name__ == '__main__':
    luigi.run()