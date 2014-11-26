''' SunGrid Engine batch-queuing system Tasks.

Originally authored by Alex Wiltschko (@alexbw) for LSF
(see https://github.com/dattalab/luigi/tree/lsf_submission/luigi)

Adapted slightly for SGE qsub/qstat commands by Jake Feala (@jfeala).
SGE is used in MIT StarCluster (http://star.mit.edu/cluster/)
and the commercial CycleCloud platform by Cycle Computing 
(http://www.cyclecomputing.com/products-solutions/cyclecloud/)

'''

 
# This extension is modeled after the hadoop.py approach.
#
# Implementation notes
# The procedure:
# - Pickle the class
# - Construct a qsub argument that runs a generic runner function with the path to the pickled class
# - Runner function loads the class from pickle
# - Runner function hits the work button on it

import os
import subprocess
import time
import sys
import logging
import random
import shutil
import cPickle as pickle

import luigi
import luigi.hadoop
from luigi import configuration
from luigi.contrib import sge_runner
from luigi.task_status import PENDING, FAILED, DONE, RUNNING, UNKNOWN

logger = logging.getLogger('luigi-interface')

POLL_TIME = 5 # decided to hard-code rather than configure here

def attach(*packages):
    logger.info("Attaching packages does nothing in SGE batch submission. All packages are expected to exist on the compute node.")
    pass

def track_job(job_id):
    '''Call qstat and parse output'''

    cmd = 'qstat'
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    qstat_out = p.communicate()[0]
    lines = qstat_out.split('\n')
    for line in lines[2:]:
        if line:
            job, prior, name, user, state = line.strip().split()[0:5]
            if int(job) == job_id:
                return state
    return 'u'


def kill_job(job_id):
    subprocess.call(['qdel', job_id])

class SGEJobTask(luigi.Task):
    """Takes care of uploading and executing an SGE job"""

    job_name = luigi.Parameter('job')
    n_cpu = luigi.IntParameter(2) #default_from_config={"section":"sge", "name":"n-cpu-flag"})
    save_job_info = luigi.BooleanParameter(default=True)
    extra_qsub_args = luigi.Parameter("")

    def fetch_task_failures(self):
        error_file = os.path.join(self.tmp_dir, "job.err")
        with open(error_file, "r") as f:
            errors = f.readlines()
        return errors

    def fetch_task_output(self):
        # Read in the output file 
        with open(os.path.join(self.tmp_dir, "job.out"), "r") as f:
            outputs = f.readlines()
        return outputs

    def _init_local(self):

        base_tmp_dir = configuration.get_config().get('sge', 'shared-tmp-dir')

        random_id = '%016x' % random.getrandbits(64)
        task_name =  random_id + self.task_id
        # If any parameters are directories, if we don't
        # replace the separators on *nix, it'll create a weird nested directory
        # SGE update: if parameters are commas, the -e and -o flags cause mysterious errors
        task_name = task_name.replace("/", "::").replace('(', '-').replace(')','').replace(', ', '-')

        
        self.tmp_dir = os.path.join(base_tmp_dir, task_name)
        # Max filename length
        max_filename_length = os.fstatvfs(0).f_namemax
        self.tmp_dir = self.tmp_dir[:max_filename_length]

        logger.info("Tmp dir: %s", self.tmp_dir)
        os.makedirs(self.tmp_dir)

        # Dump the code to be run into a pickle file
        logging.debug("Dumping pickled class")
        self._dump(self.tmp_dir)

        # Make sure that all the class's dependencies are tarred and available
        logging.debug("Tarballing dependencies")
        # Grab luigi and the module containing the code to be run
        packages = [luigi] + [__import__(self.__module__, None, None, 'dummy')]
        luigi.hadoop.create_packages_archive(packages, os.path.join(self.tmp_dir, "packages.tar"))

        # Now, pass onto the class's specified init_local() method. 
        self.init_local()

    def init_local(self):
        ''' Implement any work to setup any internal datastructure etc here.
        You can add extra input using the requires_local/input_local methods.
        Anything you set on the object will be pickled and available on the compute nodes.
        '''
        pass

    def run(self):
        self._init_local()
        self._run_job()
        # The procedure:
        # - Pickle the class
        # - Tarball the dependencies
        # - Construct a bsub argument that runs a generic runner function with the path to the pickled class
        # - Runner function loads the class from pickle
        # - Runner class untars the dependencies
        # - Runner function hits the button on the class's work() method

    def work(self):
        # Subclass this for where you're doing your actual work.
        # 
        # Why not run(), like other tasks? Because we need run to always be something that the Worker can call,
        # and that's the real logical place to do SGE scheduling. 
        # So, the work will happen in work().
        pass

    def _dump(self, out_dir=''):
        """Dump instance to file."""
        self.job_file = os.path.join(out_dir, 'job-instance.pickle')
        if self.__module__ == '__main__':
            d = pickle.dumps(self)
            module_name = os.path.basename(sys.argv[0]).rsplit('.', 1)[0]
            d = d.replace('(c__main__', "(c" + module_name)
            open(self.job_file, "w").write(d)

        else:
            pickle.dump(self, open(self.job_file, "w"))


    def _run_job(self):

        # Build a qsub argument that will run sge_runner.py on the directory we've specified.
        qsub_args = ['qsub']
        qsub_args += ['-e', '"%s"' % os.path.join(self.tmp_dir, 'job.err')] # enclose in quotes to escape parentheses in shell cmd
        qsub_args += ['-o', '"%s"' % os.path.join(self.tmp_dir, 'job.out')]
        qsub_args += ['-V'] # pass environment vars
        qsub_args += ['-r', 'y'] # re-run the job if it fails (e.g. a spot instance dies)
        qsub_args += ['-pe', 'orte', str(self.n_cpu)] # StarCluster-specific!!
        qsub_args += ['-N', self.job_name]
        qsub_args += self.extra_qsub_args.split()
        qsub_str = ' '.join(qsub_args)

        # Find where our file is
        runner_path = sge_runner.__file__
        if runner_path.endswith("pyc"):
            runner_path = runner_path[:-3] + "py"
        job_str = '''echo 'python %s "%s"' ''' % (runner_path, self.tmp_dir) 

        submit_cmd = job_str + ' | ' + qsub_str

        # That should do it. Let the world know what we're doing.
        logger.info(submit_cmd)

        # Submit the job
        p = subprocess.Popen(submit_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, cwd=self.tmp_dir, shell=True)
        output = p.communicate()[0]
        # ASSUMPTION
        # The result will be of the format
        # Your job <job_id> ("<job_name>") has been submitted
        # So get the number in those first brackets. 
        # I cannot think of a better workaround that leaves logic on the Task side of things.
        self.job_id = int(output.split()[2])
        logger.info("Job submitted as job {job_id}".format(job_id=self.job_id))

        self._track_job()

        # If we want to save the job temporaries, then do so
        # We'll move them to be next to the job output
        if self.save_job_info:
            dest_dir = self.output().path
            shutil.move(self.tmp_dir, os.path.split(dest_dir)[0])

        # Now delete the temporaries, if they're there.
        self._finish()

    def _track_job(self):
        while True:
            # Sleep for a little bit
            time.sleep(POLL_TIME)

            # See what the job's up to
            # ASSUMPTION
            sge_status = track_job(self.job_id)
            if sge_status == "r":
                job_status = RUNNING
                logger.info("Job is running...")
            elif sge_status == "qw":
                job_status = PENDING
                logger.info("Job is pending...")
            elif sge_status == "t" or sge_status == "u":
                # Then the job could either be failed or done.
                errors = self.fetch_task_failures()
                if errors == '':
                    job_status = DONE
                    logger.info("Job is done")
                else:
                    job_status = FAILED
                    logger.error("Job has FAILED")
                    logger.error("\n\n")
                    logger.error("Traceback: ")
                    for error in errors:
                        logger.error(error)
                break
            # elif sge_status == "SSUSP": # I think that's what it is...
            #     job_status = PENDING
            #     logger.info("Job is suspended (basically, pending)...")

            else:
                job_status = UNKNOWN
                logger.info("Job status is UNKNOWN!")
                logger.info("Status is : %s" % sge_status)
                raise Exception, "What the heck, the job_status isn't in my list, but it is %s" % sge_status


    def _finish(self):

        logger.info("Cleaning up temporary bits")
        if self.tmp_dir and os.path.exists(self.tmp_dir):
            logger.info('Removing directory %s' % self.tmp_dir)
            shutil.rmtree(self.tmp_dir)

    def __del__(self):
        pass
        # self._finish()


class LocalSGEJobTask(SGEJobTask):
    """A local version of SGEJobTask, for easier debugging."""

    def run(self):
        self.init_local()
        self.work()


