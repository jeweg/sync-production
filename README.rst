py-sync-production
==================

This Python (2.5-2.7) module provides a class that can act as a consumer for multiple
producers in a multi-threaded scenario, delegating their output to another consumer,
offering fine-grained control over the order of delegations.

As an example let's say we have two thread writing to ``sys.stdout``.
They will occasionally write lines that have to stay consecutive so as not to destroy
formatting. Also they might have to get user input with e.g. ``raw_input``.

Using this module, we can put a proxy object around ``sys.stdout``:

::

   from sync_production import File
   
   s = File(sys.stdout)
   sys.stdout = s

Producers can now declare their order requirements like:

::

   from sync_production import *
   
   sys.stdout.write('some output')
   # [...]

   with consecutive_mode(s):
      for i in xrange(3):
         sys.stdout.write('line %s' % i)
   # [...]

   with exclusive_mode(s):
      ok = raw_input('Continue? [y/N]:') in ['y', 'Y']

The proxy will sort output according to these modes, transparently buffering
output from other threads if it can not be delegated directly.

The ``Consumer`` class implements the functionality in a generic way and is
meant to be used as a base class.
There are premade subclasses that can forward to a file or a ``logging.Logger``,
respectively.