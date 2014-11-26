import unittest
import luigi
from luigi.contrib.sge import SGEJobTask
from subprocess import check_output
import os.path
import logging

logger = logging.getLogger('luigi-interface')

result = check_output('qhost')
assert result.startswith('HOSTNAME'), 'SunGrid Engine is not installed on this machine'


class TestJobTask(SGEJobTask, luigi.ExternalTask):

	'''Writes a test file to SGE shared drive and waits a minute'''

	shared_drive = luigi.Parameter('/home')

	def work(self):
		check_output('sleep 60', shell=True)
		with open(self.output().path, 'w') as f:
			f.write('this is a test')

	def output(self):
		return luigi.LocalTarget(os.path.join(self.shared_drive, 'testfile_' + self.job_name))


class SGETest(unittest.TestCase):

	def requires(self):
		return [TestJobTask(job_name='job_%s' % i, n_cpu=1) for i in range(3)]


if __name__ == '__main__':
	luigi.run(main_task_cls=RunAll)
