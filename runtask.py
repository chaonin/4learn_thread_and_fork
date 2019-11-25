#!/usr/bin/env python
import sys
import logging

logging.basicConfig(                                         \
level = logging.DEBUG,                                       \
filename = log_file,   \
format = "%(asctime)s: %(levelname)8s: %(name)s: %(message)s"\
)
log = logging.getLogger(__name__)
'''
cs_update_job.py job_name status pid
1. update job status : cs_update_job.py jobname status null
2.   update job pid  : cs_update_job.py jobname null   pid
3.    update job log : cs_update_job.py jobname 'log' logurl
'''

log_file = "/Users/guochaonian/PYWS/ats/4learn/daem_out"
def run_task(task_name, word, pid):
    '''
    run the task
    '''
    log_file = "/Users/guochaonian/PYWS/ats/4learn/task_" + pid + "_out"
    log.info("This is the output of task \'%s\': \n%s" % task_name, word)

if __name__ == '__main__':

    argv     = sys.argv[1:]
    task_name = argv[0]
    word = argv[1]
    
    run_task(task_name, word)
