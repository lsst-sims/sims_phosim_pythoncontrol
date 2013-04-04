#!/usr/bin/python

"""Phosim utility/convenience functions."""

from __future__ import with_statement
import csv
import datetime
import getpass
import glob
import logging
import os
import shutil
import subprocess
import time

__author__ = 'Jeff Gardner (gardnerj@phys.washington.edu)'

logger = logging.getLogger(__name__)

# ********************************************
# FILE STAGING AND ARCHIVE
# ********************************************

def ZipNameFromRaw(raw_fn):
  """Returns the proper zip archive name for a raw (i.e. e2adc) fits output."""
  zip_base = ''
  for s in raw_fn.split('.')[0].split('_'):
    if not s.startswith('C'):
      zip_base += '%s_' % s
  return zip_base.rstrip('_') + '.zip'

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

def StageFiles(source_list, dest, decompress=False, unarchive=False,
               manifest_name=None):
  """Intelligently stages files between locations.

  If manifest_name is defined, the first thing this will do is write
  a list of files that should be in the destination directory, in order
  to make file verification easier.

  Args:
    source_list:  A list of full path names of files to stage to dest.
    dest:         Full path to destination directory.
    decompress:   Optionally decompress compressed files with DecompressFileByExt()
    unarchive:    Optionally unarchive archives with UnarchiveFileByExtAndDelete()
    manifest_name: Optionally write a manifest of all files that were
                   staged to dest (does not include manifest_name itself,
                   so as to avoid Russell's paradox).
  Raises:
    OSError upon failure of file ops.
  """
  if not os.path.exists(dest):
    os.makedirs(dest)
  if manifest_name:
    if decompress or unarchive:
      raise ValueError('File manifest not supported along with decompression'
                       ' or unarchiving.')
    with open(os.path.join(dest, manifest_name), 'w') as manf:
      for source in source_list:
        manf.write('%s\n', os.path.basename(source))
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
# LOGGING
# ********************************************
def ConfigureLogging(debug_level, logfile_fullpath=None):
  """Configures the logging module output.

  Args:
    debug_level: If non-zero, write logging.DEBUG, else logging.INFO
    logfile_fullpath: full path name of logging output file.
  """
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

# ********************************************
# FILE AND PARAM MANFEST
# ********************************************
class ManifestParser(object):
  """Class for reading and writing file/param manifest.

  The suggested way to use this is with 'with', e.g.:
    with ManifestParser(manifest_filename, 'r') as parser:
      parser.Read()
      manifest = parser.Get()

  One can also supply file pointers as manifest_fp.
  """
  def __init__(self, fn=None, filemode=None):
    self.fn = fn
    self.filemode = filemode
    self.manfp = None

  def __enter__(self):
    self.Open()
    return self

  def __exit__(self, type, value, traceback):
    self.Close()

  def Open(self, fn=None, filemode=None):
    if not fn or not filemode:
      if not (self.fn and self.filemode):
        raise RuntimeError('Not enough information to open manifest.')
      fn = self.fn
      filemode = self.filemode
    self.manfp = open(fn, filemode)
    return self.manfp

  def Close(self):
    if self.manfp:
      self.manfp.close()

  def Read(self, manifest_fp=None, matcher=None):
    """Reads 2d list from manifest_fp.

    Args:
      manifest_fp: pointer to manifest file
      matcher:     Optional matcher func to filter results.

    Returns:
      2d list
    """
    if not manifest_fp:
      manifest_fp = self.manfp
    reader = csv.reader(manifest_fp)
    self.list_2d = []
    for row in reader:
      if not matcher or matcher(row):
        self.list_2d.append(tuple(row))
    return self.list_2d

  def Get(self):
    return self.list_2d

  def GetByMajor(self, major_list):
    """Return every row of manifest whose major tag is in major_list."""
    return self.GetByMatcher(lambda row: row if row[0] in major_list else None)

  def GetAllByTags(self, major_tag, minor_tag):
    """Return data in all rows that match major_tag and minor_tag."""
    return self.GetByMatcher(lambda row: row[2] if row[0] == major_tag and
                             row[1] == minor_tag else None)

  def GetLastByTags(self, major_tag, minor_tag):
    """Return data in the last row that matches major_tag and minor_tag."""
    return self.GetByMatcher(lambda row: row[2] if row[0] in major_tag and
                             row[1] in minor_tag else None)[-1]

  def GetByMatcher(self, matcher):
    """Return a list from manifest filtered by the function matcher.

    Returns:
      For every <row> in the manifest:
        A list of every output from 'matcher(<row>)' that resolves as True.
    """
    filtered_list = []
    for row in self.list_2d:
      if matcher(row):
        filtered_list.append(matcher(row))
    return filtered_list

  def ManifestFileTypeByExt(self, fn):
    if fn.endswith('.csh') or fn.endswith('.pbs'):
      return 'exec'
    elif (fn.endswith('.tar') or fn.endswith('.tgz') or fn.endswith('.tar.gz') or
          fn.endswith('.zip') or fn.endswith('.ear')):
      return 'archive'
    elif fn.endswith('.cfg'):
      return 'config'
    elif fn.endswith('.pars') or fn.endswith('.pars.gz'):
      return 'pars'
    return 'data'

  def Write(self, list_2d, manifest_fp=None):
    """Writes a 2d list to manifest.
    Args:
      list_2d:     A 2d list to write to manifest_fp.
      manifest_fp: Optional pointer to manifest file
    """
    for line in list_2d:
      logger.debug('Writing to manifest: %s', line)
    if not manifest_fp:
      manifest_fp = self.manfp
    writer = csv.writer(manifest_fp)
    writer.writerows(list_2d)
