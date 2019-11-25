#!/usr/bin/env python
import subprocess,re,os
import shlex
from subprocess import Popen,PIPE
import time, threading, Queue, StringIO
import atexit,sys
from signal import SIGTERM, SIGCHLD, SIG_IGN, SIGUSR1, SIGUSR2
import signal
import logging
import errno
import asyncore, socket

daem_server = 'localhost'
PORT        = 5555
SIZE        = 2048

class event_client(asyncore.dispatcher_with_send):
    def __init__(self, host, port, message):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((host, port))
        self.out_buffer = message

    def handle_close(self):
        self.close()

    def handle_read(self):
        data = ""
        data = self.recv(SIZE)
        #log.debug("cs web daemon - received socket data: %s" % data)
        print "received socket data: %s" % data
        self.close()

if __name__ == '__main__':  
    
    time.sleep(2)
    taskmessage = event_client(daem_server, PORT, "netstat")
    asyncore.loop()
    time.sleep(2)
    taskmessage = event_client(daem_server, PORT, "ls")
    asyncore.loop()
    time.sleep(2)
    taskmessage = event_client(daem_server, PORT, "date")
    asyncore.loop()
    time.sleep(2)

