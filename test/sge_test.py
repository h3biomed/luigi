import luigi
from luigi.contrib.sge import SGEJobTask
from subprocess import check_output
from os.path import join
import logging

logger = logging.getLogger('luigi-interface')

class TestJob(SGEJobTask, luigi.ExternalTask):

	def work(self):
		check_output('sleep 60', shell=True)
		with open('./testfile', 'w') as f:
			f.write('this is a test')
		cmd = 'aws s3 cp ./testfile s3://h3bioinf-test/test_luigi/%s' % self.job_name
		logger.info('running: ' + cmd)
		check_output(cmd, shell=True)

	def output(self):
		return luigi.S3Target('s3://h3bioinf-test/test_luigi/%s' % self.job_name)


class RunAll(luigi.WrapperTask):

	def requires(self):
		return [TestJob(job_name='job_%s' % i, n_cpu=1) for i in range(3)]


if __name__ == '__main__':
	luigi.run(main_task_cls=RunAll)