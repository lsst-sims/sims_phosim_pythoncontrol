from __future__ import with_statement
import datetime
import getpass
import glob
import logging
import os
import shutil
import subprocess
import time

logger = logging.getLogger(__name__)

# ********************************************
# FILE STAGING AND ARCHIVE
# ********************************************

def RemoveDirOrLink(dir_name):
  """Recusively deletes a directory if it is hard,  or soft link."""
  if os.path.islink(dir_name):
    os.unlink(dir_name)
  else:
    shutil.rmtree(dir_name)


def ResetDirectory(dir_name):
  """Deletes directory if it exists, then recreates it."""
  if os.path.exists(dir_name):
    shutil.rmtree(dir_name)
  os.makedirs(dir_name)

def CompressFile(fn, compression='gzip'):
  """Compresses a file.

  Args:
    fn:           Filename
    compression: 'gzip' or 'bzip2'

  Returns:
    Name of compressed file

  Raises:
    ValueError:         If unknown compression
    CalledProcessError: If archive command fails
  """
  compressed_fn = fn
  if compression in ['gzip', 'bzip2']:
    cmd = '%s %s'
    logger.info('Compressing %s with command %s', fn, cmd)
    subprocess.check_call(cmd, shell=True)
    if compression == 'gzip':
      compressed_fn = fn + '.gz'
    else:
      compressed_fn = fn + '.bz2'
  else:
    raise ValueError('Unknown compression %s' % compression)
  return compressed_fn

def DecompressFileByExt(fn):
  """Decompresses a file based on its extension.

  .gz and .bz2.  Will ignore unknown extensions.

  Args:
    fn:           Filename

  Returns:
    Name of uncompressed file

  Raises:
    CalledProcessError: If archive command fails
  """
  cmd = None
  if fn.endswith('.gz'):
    cmd = 'gunzip %s' % fn
  elif fn.endswith('.bz2'):
    cmd = 'bunzip2 %s' % fn
  if cmd:
    logger.info('Decompressing %s', fn)
    subprocess.check_call(cmd, shell=True)
    return fn.rsplit('.', 1)[0]
  return fn

def DeleteFileGlobs(globs):
  """Does a pythonic 'rm globs'.

  Args:
    globs:   A string containing a list of filenames/globs that will be
             removed, as if this were an argument to the Unix 'rm' command.

  Raises:
    OSError: if unlinking fails.
  """
  for g in globs.split():
    for fn in glob.glob(g):
      os.unlink(fn)

def ArchiveFilesByExt(fn, globs, delete_files=False):
  """Creates and archive based on extension and adds files matchings globs.

  Args:
    fn:      Name of archive.  Must end in '.tar', '.tar.gz', '.tgz',
             '.tar.bz2', '.tbz', '.tbz2', '.tb2',  or 'zip'.
    globs:   A string containing a list of filenames/globs that will be
             given as a command-line argument to the archiver executable.
    delete_files:  Optionally deletes the files that were archived.

  Returns:
    Name of archive created with full path.

  Raises:
    ValueError:         If unknown archive extension
    CalledProcessError: If archive command fails
  """
  if fn.endswith('.tar'):
    cmd = 'tar cf %s %s' % (fn, globs)
  elif fn.endswith('.tar.gz') or fn.endswith('.tgz'):
    cmd = 'tar czf %s %s' % (fn, globs)
  elif (fn.endswith('.tar.bz2') or fn.endswith('.tbz') or fn.endswith('.tb2')
        or fn.endswith('.tbz2')):
    cmd = 'tar cjf %s %s' % (fn, globs)
  elif fn.endswith('.zip'):
    cmd = 'zip %s %s' % (fn, globs)
  else:
    raise ValueError('Unknown extension for archive file %s' % fn)
  logger.info('Creating archive with command: %s', cmd)
  subprocess.check_call(cmd, shell=True)
  if delete_files:
    DeleteFileGlobs(globs)
  return os.path.abspath(fn)

def ArchiveFilesByExtAndDelete(fn, globs):
  return ArchiveFilesByExt(fn, globs, delete_files=True)

def UnarchiveFileByExtAndDelete(fn):
  """Unarchives file by extension and deletes archive."""
  UnarchiveFileByExt(fn, delete_archive=True)

def UnarchiveFileByExt(fn, delete_archive=False):
  """Unarchives file by extension.  Optionally deletes archive."""
  cmd = None
  if fn.endswith('.tar'):
    cmd = 'tar xf %s' % fn
  elif fn.endswith('.tgz'):
    cmd = 'tar xzf %s' % fn
  elif fn.endswith('.zip'):
    cmd = 'unzip %s' % fn
  if cmd:
    logger.info('Unarchiving %s', fn)
    subprocess.check_call(cmd, shell=True)
    if delete_archive:
      logger.info('Deleting %s', fn)
      os.remove(fn)

def StageFiles(source_list, dest, decompress=False, unarchive=False):
  """Intelligently stages files to execution node.

  Args:
    source_list:  A list of full path names of files to stage to dest.
    dest:         Full path to destination directory.
    decompress:   Optionally decompress compressed files with DecompressFileByExt()
    unarchive:    Optionally unarchive archives with UnarchiveFileByExtAndDelete()

  Raises:
    OSError upon failure of file ops.
  """
  if os.path.exists(dest):
    shutil.rmtree(dest)
  os.makedirs(dest)
  for source in source_list:
    logger.info('Copying %s to %s', source, dest)
    shutil.copy(source, dest)
    dest_fn = os.path.join(dest, os.path.basename(source))
    if decompress:
      DecompressFileByExt(dest_fn)
    if unarchive:
      UnarchiveFileByExtAndDelete(dest_fn)


# ********************************************
# TIMERS
# ********************************************

def RunWithWallTimer(func, name=None):
  """Runs 'func' with a timer, optionally writes time to logger.info.

  Args:
    func:   Callback for function to time.
    name:   If provided, will write a message to logger.info with
            the tag 'name'.

  Returns:
    (float) Elapsed walltime in seconds, result from func.
  """
  start_wall = time.time()
  result = func()
  interval = time.time() - start_wall
  if name:
    logger.info('TIMER[%s]: wall: %f sec', name, interval)
  return interval, result

class WithTimer:
    """http://preshing.com/20110924/timing-your-code-using-pythons-with-statement"""
    def __enter__(self):
        self.startCpu = time.clock()
        self.startWall = time.time()
        return self

    def __exit__(self, *args):
        self.interval = []
        self.interval.append(time.clock() - self.startCpu)
        self.interval.append(time.time() - self.startWall)

    def Print(self, name, stream):
      stream.write('TIMER[%s]: cpu: %f sec  wall: %f sec\n' %(name, self.interval[0],
                                                              self.interval[1]))

    def PrintCpu(self, name, stream):
      stream.write('TIMER[%s]: cpu: %f sec\n' %(name, self.interval[0]))

    def PrintWall(self, name, stream):
      stream.write('TIMER[%s]: wall: %f sec\n' %(name, self.interval[1]))

    def Log(self, name):
      logger.info('TIMER[%s]: cpu: %f sec  wall: %f sec\n', name,
                   self.interval[0], self.interval[1])

    def LogCpu(self, name):
      logger.info('TIMER[%s]: cpu: %f sec\n', name, self.interval[0])

    def LogWall(self, name):
      logger.info('TIMER[%s]: wall: %f sec\n', name, self.interval[1])


# ********************************************
# LOGGERS
# ********************************************
def ConfigureLogging(debug_level, logfile_fullpath=None):
  if logfile_fullpath:
    if not os.path.exists(os.path.dirname(logfile_fullpath)):
      os.makedirs(os.path.dirname(logfile_fullpath))
  log_format = '%(asctime)s %(levelname)s:%(name)s:  %(message)s'
  log_level = logging.DEBUG if debug_level else logging.INFO
  logging.basicConfig(filename=logfile_fullpath, filemode='w', level=log_level,
                      format=log_format)

def WriteLogHeader(name, params_str='', stream=None):
  """Write log header.

  Args:
    name:        Name of script (e.g. 'fullFocalPlane', etc).  Typically,
                 just pass __file__.
    params_str:  A string containing any parameters you would like to document.
    stream:      Write to this stream instead of logger.
  """
  header = ('\n#################################################################\n'
            'Logfile created by: %s\n'
            '%s\n'
            'Run by:    %s\n'
            'Run on:    %s\n'
            '#################################################################\n' %
            (name, params_str, getpass.getuser(), str(datetime.datetime.now())))
  if stream:
    stream.write(header)
  else:
    logger.info(header)
  return
