#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import with_statement

import sys
import thread
import threading
import logging
from contextlib import contextmanager

__all__ = ['Consumer', 'File', 'Logger',
           'consecutive_mode', 'exclusive_mode']

###################################################################

class _Buffer:
    def __init__(self, drop_asap):
        self.drop_asap = drop_asap
        self.buffer_data = []


class Consumer(object):
    """This class can act as a consumer for multiple producers in a
    multi-threaded scenario, delegating their output to another consumer,
    offering fine-grained control over the order of delegations.
    
    It tries to go beyond a simple locking approach by using transparent
    per-thread output buffering if necessary to block threads as little as
    possible.    
    While not in any way limited to it, a typical use case would  be text
    output from several worker threads to the console
    (or a file, a logger, ...), where parts of the output must stay consecutive
    due to formatting, and threads may need to exclude the others for some
    duration while waiting for user input.
    These requirements are controlled by two context managers.

    This class implements the functionality in a generic way and is not meant
    to be used directly, but serves as a base class.
    There are premade subclasses that can forward to a file or a logger,
    respectively. To use it in a different context, inherit Consumer and
    overwrite _Consumer__input_to_sync and _Consumer__output_from_sync.
    Call the first method to delegate output from the producers to the
    synchronizer. The latter method will then be called (repeatedly) to
    delegate synchronized output further.

    The way this works is with dynamically managed per-thread buffers.
    In consecutive mode outputs go to a buffer until the block is exited at
    which time the buffer is emptied. During that time other threads may
    output directly. In exclusive mode, one thread gains unbuffered
    access while all others are buffered. When a thread releases exclusivity,
    the other threads' buffers are emptied if they were buffered due to the
    exclusive mode.
    """

    def __init__(self, *args, **kwargs):
        """Inits a consumer object. Pass max_buffer_size as a keyword
        argument to set the maximum element number for each of the buffers.
        It will default to unbounded buffers.
        """
        self.__max_buffer_size = kwargs.get('max_buffer_size', sys.maxint)

        # Thread ident -> _Buffer object
        self.__buffers = {}
        
        # Syncs access to the buffers dict itself when we operate on it
        # across multiple statements.
        # It's also used to effectively sync __drop_buffer() with its
        # nested calls to __input_to_sync (that's why it's an reentrant lock).
        # This would otherwise be an opportunity for race conditions..
        self.__buffers_lock = threading.RLock()

        # Serializes multiple threads asking for exclusive_mode.
        self.__exclusivity_lock = threading.Lock()
        
        # Thread ident of the current excl. holder, if any.
        self.__exclusivity_holder = None

        # For the context managers to keep track of their
        # per-thread nesting depth.
        self.__keep_consecutive_depth = {}
        self.__exclusivity_depth = {}


    def __output_from_sync(self, thing, thread_ident):
        """This function is called from the Consumer internals to actually
        pass one thread's output. It should be overwritten in subclasses
        to pass the output to the actual sink."""
        raise NotImplementedError()


    def __input_to_sync(self, thing):
        """Call this function to send producer output to the Consumer
        internals.""" 

        ident = thread.get_ident()
        force_buffering = False

        if self.__exclusivity_lock.locked():
            if ident == self.__exclusivity_holder:
                # We are the Chosen!
                self.__output_from_sync(thing, ident)
                return
            force_buffering = True

        with self.__buffers_lock:
            buff = self.__buffers.get(ident, None)
            if buff is None and force_buffering:
                # Buffer because of exclusive mode, it should be
                # dropped asap.
                buff = _Buffer(True)
                self.__buffers[ident] = buff

        if buff is not None:
            # Add to buffer:
            if (hasattr(self, '_Consumer__max_buffer_size') and
                len(buff.buffer_data) == self.__max_buffer_size):
                # The buffer is full!
                # Now we have no choice but to empty the buffer.
                # We can do this right after exclusivity mode ends.
                # Setting the drop_asap flag
                # will have our buffer dropped automatically once the
                # mode ends. So it's enough here to wait for the lock
                # and release it again.
                buff.drop_asap = True
                self.__exclusivity_lock.acquire()
                self.__exclusivity_lock.release()
            else:
                buff.buffer_data.append(thing)
        else:
            # Unbuffered output:
            self.__output_from_sync(thing, ident)
    
    
    def __drop_buffer(self, ident):
        with self.__buffers_lock:
            buff = self.__buffers.get(ident, None)
            if buff is None:
                return
            # Remove from dict first so __input_to_sync()
            # does not in turn buffer again.
            del self.__buffers[ident]
            for entry in buff.buffer_data:
                self.__input_to_sync(entry)


    def __make_consecutive(self, ident, on_off):
        if on_off:
            with self.__buffers_lock:
                buff = self.__buffers.get(ident, None)
                if buff is not None:
                    # If there already was a buffer, it was created due to
                    # another thread's exclusive mode. Obviously this current
                    # thread must keep buffering, but it must not be dropped
                    # asap anymore since that could destroy consecutivity.
                    buff.drop_asap = False
                else:
                    self.__buffers[ident] = _Buffer(False)
        else:
            self.__drop_buffer(ident)


    def __set_exclusivity(self, ident):
        # Assume that resetting this is always since the
        # context manager should call this.
        if ident is None:

            # Purge all buffers that exist just for the ending
            # exclusive_mode mode.
            with self.__buffers_lock:
                all_idents = self.__buffers.keys()
                for i in all_idents:
                    if self.__buffers[i].drop_asap:
                        self.__drop_buffer(i)

            self.__exclusivity_holder = None
            self.__exclusivity_lock.release()
            return
        
        # Wait until noone else has exclusive access, then grab power!
        self.__exclusivity_lock.acquire()
        self.__exclusivity_holder = ident
        # If buffer exists for me already: purge it,
        # we have access after all.
        self.__drop_buffer(ident)


###################################################################

@contextmanager
def consecutive_mode(consumer):
    """Context manager to indicate that outputs from the current
    thread to the consumer should stay consecutive, even if other threads
    produce output in between. The way this works is that the consecutive
    output goes to a buffer that is emptied atomically at the end of the
    context. This ensures that output from other threads can still come through
    with minimal latency. If you need consecutive output that arrives
    as soon as it is produced, simply use exclusive mode.
    """
    ident = thread.get_ident()
    # Consecutive mode used inside exclusive mode
    # simply does nothing. This simplifies our logic and
    # obviously does not change the behaviour.
    c = consumer._Consumer__exclusivity_depth.get(ident, 0)
    if c > 0:
        yield
        return

    c = consumer._Consumer__keep_consecutive_depth.get(ident, 0)
    if not c:
        consumer._Consumer__make_consecutive(ident, True)
    consumer._Consumer__keep_consecutive_depth[ident] = c + 1
    try:
        yield
    finally:
        consumer._Consumer__keep_consecutive_depth[ident] -= 1
        if not c:
            consumer._Consumer__make_consecutive(ident, False)

###################################################################

@contextmanager
def exclusive_mode(consumer):
    """Context manager to indicate that any outputs from the current
    thread to the consumer should be unbuffered and exclusive,
    i.e. output from any other threads will go to buffers.
    If a thread requests exclusive mode while another holds
    exclusivity on the consumer object, the thread is blocked until
    the other thread's exclusive mode has ended.
    """
    ident = thread.get_ident()
    c = consumer._Consumer__exclusivity_depth.get(ident, 0)
    if not c:
        consumer._Consumer__set_exclusivity(ident)
    consumer._Consumer__exclusivity_depth[ident] = c + 1
    try:
        yield
    finally:
        consumer._Consumer__exclusivity_depth[ident] -= 1
        if not c:
            consumer._Consumer__set_exclusivity(None)

###################################################################

class File(Consumer):
    """An output-file-like object with the bufferd synchronization
    functionality."""

    def __init__(self, target_file,
                 max_buffer_size=sys.maxint):
        super(File, self).__init__(max_buffer_size=max_buffer_size)
        self.target_file = target_file

    def flush(self):
        self.target_file.flush()

    def write(self, string):
        self._Consumer__input_to_sync(string)

    def _Consumer__output_from_sync(self, string, thread_ident):
        self.target_file.write(string)

###################################################################

# I cannot inherit from Logger because Logger objects are created by
# an internal(?) factory function. So this thing forwards, first internally,
# and then to the target logger.
class Logger(Consumer):
    """A wrapper for logging.Logger objects with the bufferd synchronization
    functionality."""

    def __init__(self, target_logger,
                 max_buffer_size=sys.maxint):
        super(Logger, self).__init__(max_buffer_size=max_buffer_size)
        self.target_logger = target_logger

    def setLevel(self, level):
        self.target_logger.setLevel(level)

    def addHandler(self, handler):
        self.target_logger.addHandler(handler)

    def debug(self, msg, *args, **kwargs):
        self._log(logging.DEBUG, msg, args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self._log(logging.INFO, msg, args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._log(logging.WARNING, msg, args, **kwargs)
    warn = warning

    def error(self, msg, *args, **kwargs):
        self._log(logging.ERROR, msg, args, **kwargs)

    def exception(self, msg, *args):
        self.error(msg, exc_info=1, *args)

    def critical(self, msg, *args, **kwargs):
        self._log(logging.CRITICAL, msg, args, **kwargs)
    fatal = critical

    def log(self, level, msg, *args, **kwargs):
        self._log(level, msg, args, **kwargs)

    def _log(self, level, msg, args, exc_info=None, extra=None):
        self._Consumer__input_to_sync((level, msg, args, exc_info, extra))

    def _Consumer__output_from_sync(self, thing, thread_ident):
        self.target_logger._log(*thing)

###################################################################

if __name__ == '__main__':

    """This example uses the buffered synchronization to sync the output
    stdout output of several threads. Each thread has different preferences:
    some want all or parts of their output to stay consecutive, some need
    exclusivity to get output from the user. In contrast to using a global
    lock to serialize access to stdout this system will make sure the
    requirements are met while the threads are blocked as little as possible.
    For example, while one thread waits for user input the others can still
    run and produce output. The only thread that really has to be blocked is
    the other one waiting for exclusivity.
    
    In the example, the 'a' lines have no special requirements,
    the 'b' lines should be consecutive within groups of 3, the 'c' lines
    should be all consecutive, the 'd' lines have no requirements, but will
    perform a raw_input in the middle. 'e' will do the same, but will also
    want the first 6 lines to stay consecutive with that.
    A possible outcome of these requirements would be:

    dddd0
    aaaaaaaaaaaaa0
    aaaaaaaaaaaaa1
    aaaaaaaaaaaaa2
    dddd1
    dddd2
    aaaaaaaaaaaaa3
    bbbbbbbbbb0
    bbbbbbbbbb1
    bbbbbbbbbb2
    aaaaaaaaaaaaa4
    dddd3
    aaaaaaaaaaaaa5
    bbbbbbbbbb1
    bbbbbbbbbb3
    bbbbbbbbbb5
    dddd4
    aaaaaaaaaaaaa6
    e0
    e1
    e2
    e3
    e4
    e5
    e  .
    e ...
    e Your input, Sir? >testing
    e ...
    e  .
    dddd5
    ccccccc0
    ccccccc1
    ccccccc2
    ccccccc3
    ccccccc4
    ccccccc5
    ccccccc6
    ccccccc7
    ccccccc8
    ccccccc9
    bbbbbbbbbb2
    bbbbbbbbbb5
    bbbbbbbbbb8
    aaaaaaaaaaaaa7
    aaaaaaaaaaaaa8
    aaaaaaaaaaaaa9
    dddd  .
    adddd ...
    dddd Your input, Sir? >one more input...
    dddd ...
    dddd  .
    e6
    e7
    e8
    e9
    dddd6
    dddd7
    dddd8
    dddd9
    """

    import random
    import time
    
    # Replace sys.stdout with our wrapper.
    s = File(sys.stdout)
    sys.stdout = s

    # A slow and jittery write() to give the threads plenty of
    # opportunity to present us with race conditions.
    def write(string):
        time.sleep(random.random() * 0.1)
        sys.stdout.write(string)

    # Define 5 functions which we will run in parallel:

    # This task don't care about no ordering:
    def task1(name):
        for i in xrange(10):
            write('%s%s\n' % (name, i))

    # Consecutive within each section of 3 lines:
    def task2(name):
        for i in xrange(3):
            with consecutive_mode(s):
                for j in xrange(3):
                    write('%s%s\n' % (name, (i+1) * (j+1) - 1))

    # All onsecutive output, senselessly nested for fun:
    def task3(name):
        with consecutive_mode(s):
            with consecutive_mode(s):
                with consecutive_mode(s):
                    for i in xrange(5):
                            write('%s%s\n' % (name, i))
                for i in xrange(5, 10):
                    write('%s%s\n' % (name, i))

    # Exclusivity in the middle:
    def task4(name):
        for i in xrange(10):
            write('%s%s\n' % (name, i))
            if i == 5:
                with exclusive_mode(s):
                    write('%s  .\n' % name)
                    write('%s ...\n' % name)
                    raw_input('%s Your input, Sir? >' % name)
                    write('%s ...\n' % name)
                    write('%s  .\n' % name)

    # Mixing it all:
    def task5(name):
        with consecutive_mode(s):
            for i in xrange(6):
                write('%s%s\n' % (name, i))
                if i == 5:
                    with exclusive_mode(s):
                        with consecutive_mode(s):
                            write('%s  .\n' % name)
                            write('%s ...\n' % name)
                        raw_input('%s Your input, Sir? >' % name)
                        write('%s ...\n' % name)
                        write('%s  .\n' % name)
        for i in xrange(6, 10):
            write('%s%s\n' % (name, i))

    # Start a thread for each task
    for name, func in [('aaaaaaaaaaaaa', task1),
                       ('bbbbbbbbbb', task2),
                       ('ccccccc', task3),
                       ('dddd', task4),
                       ('e', task5)]:
        threading.Thread(name=name, target=func, args=(name,)).start()
