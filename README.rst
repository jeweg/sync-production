py-sync-production
==================

This Python module provides a class that can act as a consumer for multiple
producers in a multi-threaded scenario, delegating their output to another consumer,
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
