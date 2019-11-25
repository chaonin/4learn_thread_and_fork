import subprocess,re,os
import shlex
from subprocess import Popen,PIPE
import time, threading, Queue, StringIO
import atexit,sys
from signal import SIGTERM, SIGCHLD, SIG_IGN, SIGINT, SIGKILL, SIGSTOP, SIGCONT
import signal
import logging
import asyncore, socket

log_file = "/Users/guochaonian/PYWS/ats/4learn/daem_out"
err_file = "/Users/guochaonian/PYWS/ats/4learn/daem_err"
pid_file = "/Users/guochaonian/PYWS/ats/4learn/daem_pid"

logging.basicConfig(                                         \
level = logging.DEBUG,                                       \
filename = log_file,   \
format = "%(asctime)s: %(levelname)8s: %(name)s: %(message)s"\
)
log = logging.getLogger(__name__)

daem = 'localhost'
SIZE = 2048
PORT = 5555
task_queue_hash = {}
task_scheduler_hash = {}
task_scheduler_init_hash = {}
task_scheduler_join_hash = {}

task_handle_file = "runtask"

class daemon:
    """
    daemon class for job scheduler
    """
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        self.event_server = ''
        
    def daemonize(self):
        """
        do the UNIX double-fork magic
        """
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                cpid = str(os.getpid())
                #log.debug("this is parent process, process PID = %s" % cpid)
                #log.debug("This is the 1st parent process, child process PID = %d" % pid)
                sys.exit(0)
            elif pid == 0:
                pass
                #log.debug("This is the 1st child process, PID = %s" % str(os.getpid()))
        except OSError, e:
            log.error("daem - fork #1 failed: %d (%s)" % (e.errno, e.strerror))
            sys.exit(1)
            
        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)
                
        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                #log.debug("This is the 2nd parent process, child process PID = %d" % pid)
                sys.exit(0)
            elif pid == 0:
                #log.debug("This is the 2st child process, PID = %s" % str(os.getpid()))
                pass
        except OSError, e:
            log.error("daem - fork #2 failed: %d (%s)" % (e.errno, e.strerror))
            sys.exit(1)
                    
        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
        
        # write pidfile
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile,'w+').write("%s\n" % pid)
    
        log.info('daem started')

    def start_event_server(self):
        self.event_server = event_server((daem, PORT)) 
        self.event_server.serve_forever()

    def total_task_no(self):
        tasks = task_queue_hash.keys()
        for task in tasks:
            self.task_no += task_queue_hash[task].qsize()
        log.debug("daem - totally %d tasks" % self.task_no)

    def delpid(self):
        os.remove(self.pidfile)

    def sig_debug(self):
        signal.signal(signal.SIGUSR1, debug_handler) 

    def sig_undebug(self):
        signal.signal(signal.SIGUSR2, undebug_handler) 
            
    def start(self):
        """
        Start the daemon
        """
        # check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
       
        if pid:
            message = "%s: cs ats daemon - pidfile %s already exist. Daemon already running?\n"
            sys.stderr.write(message % (time.ctime(), self.pidfile))
            sys.exit(1)
               
        # init & start the daemon
        self.daemonize()
        self.sig_debug()
        self.sig_undebug()
        self.start_event_server()
            
    def stop(self):
        """
        Stop the daemon
        """
        # get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
       
        if not pid:
            message = "%s: daem-pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % (time.ctime(), self.pidfile))
            return
 
        # try killing the daemon process       
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                sys.stderr.write("%s: daem - killing daemon error: %s" % \
                    (time.ctime(), err))
                sys.exit(1)

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()

class task_scheduler(threading.Thread):

    """
    Threaded task scheduler 
    """

    def __init__(self, name, queue,):
        threading.Thread.__init__(self)
        self.name = 'daem_thread_' + name
        self.queue = queue
        
    def run(self):
        while True:
            # get task 
            task = self.queue.get()
            log.info("daem - deque task: %s" % task.get_task_name())
            task.run_task()
            self.queue.task_done()

def debug_handler(SIG, FRM):
    """
    signal handler for debug request
    """
    log.info("daem - debug level update to 'DEBUG'")
    logging.root.setLevel(logging.DEBUG)
    return 1

def undebug_handler(SIG, FRM):
    """
    signal handler for undebug request
    """
    log.info("daem - debug level update to 'INFO'")
    logging.root.setLevel(logging.INFO)
    return 1

class task:
    '''
    the detailed task 
    '''
    def __init__(self, taskname, word):
        self.task_name = taskname 
        self.word_to_print = word 
        self.task_pid = ""
        self.task_cmd = word 

    def get_task_pid(self):
        return self.task_pid

    def get_task_name(self):
        return self.task_name

    def run_task(self):
        # get task status from cs_ucm_busy_flag
        log.debug("daem - running task: %s" % self.task_name)

        #task_cmd be a list, so no need 'shell=True', which will not call '/bin/sh', 
        #and ppid of the sub-process(else, the ppid will be '/bin/sh')will be this 
        #process's pid;if task_cmd a string, 'shell=True' must be specific.
        handler = Popen(self.task_cmd, universal_newlines=True, stderr=PIPE)
        self.task_pid = handler.pid
        log.debug("daem - forking process: %s" % self.task_pid)

        # update task pid to real one 
        # comment out by chaonin 2019.11.12, becoz task_cmd is just a single word here!
        """
        run_task = Popen(["python", task_handle_file, self.task_name, self.word_to_print, str(self.task_pid)], \
            stdout=PIPE, stderr=PIPE)
        out, err = run_task.communicate()
        if err is not '': # give an error log, go on exec
            log.warning("daem - failed to run task \'%s\': %s" \
                % (self.task_name, str(dberr)))
        """

        # error handle: communicate return an stdout/stderr tuple
        error = handler.communicate()[1]
        if error != '': 
            log.error("daem - task exec failed: %s" % str(error))

        handler.wait() #wait for child process to end(avoid zombie)
        log.debug("cs ats daem - ending task: %s" % self.task_name)

class event_handler(asyncore.dispatcher):
    '''
    event handler for detailed task 
    '''
    def __init__(self, conn_sock, client_address, server):
        self.server             = server
        self.client_address     = client_address
        self.buffer             = ""

        # we dont have anything to write, to start with
        self.is_writable        = False

        # create ourselves with conn_sock
        asyncore.dispatcher.__init__(self, conn_sock)
        log.debug("daem - handler created")

    def paramGetValue(self, param, argsList):
        ''' 
        get param value from argsList, argsList is not a standard '-param value' style:
        'csh ...apper.csh {good ones} -phones phone1 phone2 phone3 {good ones} > ...log'  
        we just got the good ones' params' value 
        '''
        try:
            index = argsList.index(param)  
        except ValueError as e:
            raise e 
        return argsList[index + 1]

    def readable(self):
        return True     # we are always happy to read

    def writable(self):
        return self.is_writable # send only there is sth to
    
    def schedule_task(self, task_name, word):
        '''
        main event handler
        '''
        # charater, digit, special charater...
        utask = task(task_name, word)
        task_type = "command"

        # each task_type use a FIFO queue
        if task_type not in task_queue_hash.keys():
            task_queue_hash[task_type] = Queue.Queue()
        log.debug("daem - enque job: %s" % task_name)
        task_queue_hash[task_type].put(utask)
        
        #init threads if have not been there
        task_handlers = task_queue_hash.keys()
        inits = task_scheduler_init_hash.keys()
        for task_handler in task_handlers:
            if task_handler not in inits:
                queue = task_queue_hash[task_handler] 
                task_scheduler_hash[queue] = task_scheduler(task_handler, queue)
                #log.info("aaaaaaaaaaa")
                log.debug("daem - init thread: %s" % \
                    task_scheduler_hash[queue].getName())
                task_scheduler_hash[queue].setDaemon(True)
                task_scheduler_hash[queue].start()
                task_scheduler_init_hash[task_handler] = 1
            else:
                pass    

        # block the thread until jobs finish if not 
        joins = task_scheduler_join_hash.keys()
        for task_handler in task_handlers:
            if task_handler not in joins:
                task_queue_hash[ucm].join
                task_scheduler_join_hash[task_handler] = 1
        
    def kill_task(self, task_name):
        # to do 
        hello = "world"

    def pause_task(self, task_name):
        # to do 
        hello = "world"

    def continue_task(self, task_name):
        # to do 
        hello = "world"

    def unschedule_task(self, task_name):
        # to do 
        hello = "world"

    def handle_read(self):
        '''
        event handler for task input
        '''
        data = self.recv(SIZE)
        if data:
            ret = data + " received"
            self.buffer += ret 
            log.debug("daem - received task \'%s\'" % data)
            self.schedule_task(data, data)
            self.is_writable = True  # sth to send back now
            log.info("aaaaaaabbbbb")
        else:
            log.debug("daem - get null task")

    def handle_write(self):
        #log.debug("handle_write")
        if self.buffer:
            sent = self.send(self.buffer)
            #log.debug("sent data")
            self.buffer = self.buffer[sent:]
        else:
            #log.debug("nothing to send")
            pass
        if len(self.buffer) == 0:
            self.is_writable = False

    # Will this ever get called?  Does loop() call
    # handle_close() if we called close, to start with?
    def handle_close(self):
        log.debug("daem - event handler closed")
        self.close()

class event_server(asyncore.dispatcher):
    '''
    event server
    ''' 
    allow_reuse_address         = False
    request_queue_size          = 5
    address_family              = socket.AF_INET
    socket_type                 = socket.SOCK_STREAM

    def __init__(self, address, handlerClass = event_handler):
        self.address            = address
        self.handlerClass       = handlerClass

        asyncore.dispatcher.__init__(self)
        self.create_socket(self.address_family,
                               self.socket_type)

        if self.allow_reuse_address:
            self.set_resue_addr()

        self.server_bind()
        self.server_activate()

    def server_bind(self):
        self.bind(self.address)
        log.info("daem - event server bind address: %s:%s" % \
            (self.address[0], self.address[1]))

    def server_activate(self):
        self.listen(self.request_queue_size)

    def fileno(self):
        return self.socket.fileno()

    def serve_forever(self):
        asyncore.loop()

    # todo: try to implement handle_request()

    # Internal use
    def handle_accept(self):
        (conn_sock, client_address) = self.accept()
        if self.verify_request(conn_sock, client_address):
            self.process_request(conn_sock, client_address)

    def verify_request(self, conn_sock, client_address):
        return True

    def process_request(self, conn_sock, client_address):
        log.debug("daem - get request from: %s:%s" % \
            (client_address[0], client_address[1]))
        self.handlerClass(conn_sock, client_address, self)

    def handle_close(self):
        self.close()



if __name__ == '__main__':  

    core = daemon(\
                  pid_file,\
                  '/dev/null',\
                  log_file,\
                  err_file)

    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            core.start()
        if 'restart' == sys.argv[1]:
            core.restart()
        if 'stop' == sys.argv[1]:
            core.stop()
            if os.path.exists('daem_out'):
                os.remove('daem_out') # chaoguo, just for next test 
            if os.path.exists('daem_err'):
                os.remove('daem_err') # chaoguo, just for next test 
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)
