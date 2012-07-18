import logging
import logging.config
import os
import signal
import sys
import time
from multiprocessing import Process, Pipe
from threading import Thread

logging.config.dictConfig({
    'formatters': {
        'brief': {
            'format': '%(levelname)-7s %(name)s - %(message)s',
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'brief',
        },
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['console'],
    },
    #'disable_existing_loggers': False,
    'version': 1,
})

log = logging.getLogger()

def master():
    results = []
    mcn, scn = Pipe()
    mon = Thread(target=monitor, args=(mcn, results))
    mon.daemon = True
    mon.start()

    proc = Process(target=slave, args=(scn,))
    proc.start()

    mcn.send('1')
    while not results:
        time.sleep(0.01)
    pid = results.pop()
    log.info('pid: %s', pid)

    mcn.send('die')
    try:
        for n in range(7):
            if mcn.poll(1):
                break
            if not proc.is_alive():
                raise Exception('stop')
    except Exception:
        log.error('slave died', exc_info=True)

    #os.kill(pid, signal.SIGKILL)
    #mcn.poll()
    #proc.terminate()

def monitor(cn, results):
    try:
        while True:
            results.append(cn.recv())
            log.info('monitor got %s', results[0])
    except:
        log.error('montor died', exc_info=True)


def slave(cn):
    try:
        while True:
            item = cn.recv()
            log.info('slave got %s' % item)
            if item == 'die':
                sys.exit()
            else:
                cn.send(os.getpid())
    except:
        log.error('slave died', exc_info=True)

if __name__ == '__main__':
    master()
