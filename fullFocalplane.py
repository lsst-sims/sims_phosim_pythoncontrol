#!/usr/bin/python

"""Perform preprocessing and generate raytrace exec scripts for one focal plane.

For documentation using the python_control for ImSim/PhoSim version <= v.3.0.x,
see README.v3.0.x.txt.

For documentation using the python_control for ImSim/PhoSim version == v.3.2.x,
see README.txt.

The behavior of this script differs depending on the version of ImSim/PhoSim.
For versions <= v3.0.x, it functions like the original fullFocalplane.py and
calls AllChipsScriptGenerator.makeScripts() to generate a script and some tarballs
that can in turn be executed to run the preprocessing step (which in turn calls
AllChipsScriptGenerator) to generate shells scripts and tarballs for performing
the raytrace stage.  See README.v3.0.x.txt for more info.

The behavior for ImSim/PhoSim version == 3.2.x is to run the preprocessing step
directly through the class PhosimManager.PhosimPrepreprocessor (which in turn
calls phosim.py in the phosin.git repository).  After the preprocessing is
complete, PhosimPreprocessor generates shell scripts for the raytrace phase.

A few notes on options:
  --skip_atmoscreens: Use this to optionally skip the step to generate atmosphere
                      screens during preprocessing and instead perform this
                      operation at the start of the raytrace phase.  This is
                      useful in distributed environments where the cost of
                      transferring the atmosphere screens to the compute node
                      is higher than recalculating them.

  --logtostderr: (only v3.2.x and higher) By default, log output from python_controls
                 is done via the python logging module, and directed to either
                 log_dir in the imsim_config_file or /tmp/fullFocalplane.log
                 if log_dir is not specified.  This option overrides this behavior
                 and prints logging information to stdout.  Note: output from
                 phosim.py and the phosim binaries are still printed to stdout.

TODO(gardnerj): Add stdout log redirect
TODO(gardnerj): Support sensor_ids argument for phosim.py.
TODO(gardnerj): Support not running e2adc step.
"""

from __future__ import with_statement
import ConfigParser
from distutils import version
import logging
from optparse import OptionParser  # Can't use argparse yet, since we must work in 2.5
import os
import sys
from AllChipsScriptGenerator import AllChipsScriptGenerator
import PhosimManager
import PhosimUtil
import PhosimVerifier
import ScriptWriter

__author__ = 'Jeff Gardner (gardnerj@phys.washington.edu)'

logger = logging.getLogger(__name__)

def DoPreprocOldVersion(trimfile, policy, extra_commands, scheduler, sensor_id):
  """Do preprocessing for v3.1.0 and earlier.

  Args:
    trimfile:          Full path to trim metadata file.
    policy:            ConfigParser object from python_controls config file.
    extra_commands:    Full path to extra commands or 'extraid' file.
    scheduler:         Name of scheduler (currently, just 'csh' is supported).
    sensor_id:         If not '', run just this single sensor ID.

  Returns:
    0 (success)
  """
  with PhosimUtil.WithTimer() as t:
    # Determine the pre-processing scheduler so that we know which class to use
    if scheduler == 'csh':
      scriptGenerator = AllChipsScriptGenerator(trimfile, policy, extra_commands)
      scriptGenerator.makeScripts(sensor_id)
    elif scheduler == 'pbs':
      scriptGenerator = AllChipsScriptGenerator_Pbs(trimfile, policy, extra_commands)
      scriptGenerator.makeScripts(sensor_id)
    elif scheduler == 'exacycle':
      print 'Exacycle funtionality not added yet.'
      return 1
    else:
      print 'Scheduler "%s" unknown.  Use -h or --help for help.' % scheduler
  t.LogWall('makeScripts')
  return 0

def DoPreproc(trimfile, imsim_config_file, extra_commands, scheduler,
              skip_atmoscreens=False, keep_scratch_dirs=False):
  """Do preprocessing for v3.2.0 and later.

  Args:
    trimfile:          Full path to trim metadata file.
    imsim_config_file: Full path to the python_controls config file.
    extra_commands:    Full path to extra commands or 'extraid' file.
    scheduler:         Name of scheduler (currently, just 'csh' is supported).
    skip_atmoscreens:  Generate atmosphere screens in raytrace stage instead
                       of preprocessing stage.
    keep_scratch_dirs: Do not delete the working directories at the end of
                       execution.

  Returns:
    0 upon success, 1 upon failure.
  """
  if scheduler == 'csh':
    preprocessor = PhosimManager.Preprocessor(imsim_config_file,
                                              trimfile, extra_commands)
  elif scheduler == 'pbs':
    # Construct PhosimPreprocessor with PBS-specific ScriptWriter
    preprocessor = PhosimManager.Preprocessor(
      imsim_config_file, trimfile, extra_commands,
      script_writer_class=ScriptWriter.PbsRaytraceScriptWriter)
    # Read in PBS-specific config
    policy = ConfigParser.RawConfigParser()
    policy.read(imsim_config_file)
    preprocessor.script_writer.ParsePbsConfig(policy)

  else:
      logger.critical('Unknown scheduler: %s. Use -h or --help for help',
                       scheduler)
      return 1
  preprocessor.InitExecEnvironment()
  with PhosimUtil.WithTimer() as t:
    if not preprocessor.DoPreprocessing(skip_atmoscreens=skip_atmoscreens):
      logger.critical('DoPreprocessing() failed.')
      return 1
  t.LogWall('DoPreprocessing')
  exec_manifest_fn = 'execmanifest_raytrace_%s.txt' % preprocessor.focalplane.observationID
  files_to_stage = preprocessor.ArchiveRaytraceInputByExt(exec_archive_name=exec_manifest_fn)
  if not files_to_stage:
    logger.critical('Output archive step failed.')
    return 1
  with PhosimUtil.WithTimer() as t:
    preprocessor.StageOutput(files_to_stage)
  t.LogWall('StageOutput')
  if not keep_scratch_dirs:
    preprocessor.Cleanup()
  verifier = PhosimVerifier.PreprocVerifier(imsim_config_file, trimfile,
                                            extra_commands)
  missing_files = verifier.VerifySharedOutput()
  if missing_files:
    logger.critical('Verification failed with the following files missing:')
    for fn in missing_files:
      logger.critical('   %s', fn)
    sys.stderr.write('Verification failed with the following files missing:\n')
    for fn in missing_files:
      sys.stderr.write('   %s\n', fn)
  else:
    logger.info('Verification completed successfully.')
  return 0

def ConfigureLogging(trimfile, policy, log_to_stdout, imsim_config_file,
                     extra_commands=None):
  """Configures logger.

  If log_to_stdout, the logger will write to stdout.  Otherwise, it will
  write to:
     'log_dir' in the config file, if present
     /tmp/fullFocalplane.log if 'log_dir' is not present.
  Stdout from phosim.py and PhoSim binaries always goes to stdout.
  """
  if log_to_stdout:
    log_fn = None
  else:
    if policy.has_option('general', 'log_dir'):
      # Log to file in log_dir
      obsid, filter_num = PhosimManager.ObservationIdFromTrimfile(
        trimfile, extra_commands=options.extra_commands)
      log_dir = os.path.join(policy.get('general', 'log_dir'), obsid)
      log_fn = os.path.join(log_dir, 'fullFocalplane_%s.log' % obsid)
    else:
      log_fn = '/tmp/fullFocalplane.log'
  PhosimUtil.ConfigureLogging(policy.getint('general', 'debug_level'),
                              logfile_fullpath=log_fn)
  params_str = 'trimfile=%s\nconfig_file=%s\n' % (trimfile, imsim_config_file)
  if extra_commands:
    params_str += 'extra_commands=%s\n' % extra_commands
  PhosimUtil.WriteLogHeader(__file__, params_str=params_str)

def main(trimfile, imsim_config_file, extra_commands, skip_atmoscreens,
         keep_scratch_dirs, sensor_ids, log_to_stdout=False):

  """
  Run the fullFocalplanePbs.py script, populating it with the
  correct user and cluster job submission information from an LSST
  policy file.
  """

  policy = ConfigParser.RawConfigParser()
  policy.read(imsim_config_file)
  if policy.has_option('general', 'phosim_version'):
    phosim_version = policy.get('general', 'phosim_version')
  else:
    phosim_version = '3.0.1'
  ConfigureLogging(trimfile, policy, log_to_stdout,
                   imsim_config_file, extra_commands)
  # print 'Running fullFocalPlane on: ', trimfile
  logger.info('Running fullFocalPlane on: %s ', trimfile)

  # print 'Using Imsim/Phosim version', phosim_version
  logger.info('Using Imsim/Phosim version %s', phosim_version)
  # Must pass absolute paths to imsim/phosim workers
  if not os.path.isabs(trimfile):
    trimfile = os.path.abspath(trimfile)
  if not os.path.isabs(imsim_config_file):
    imsim_config_file = os.path.abspath(imsim_config_file)
  if not os.path.isabs(extra_commands):
    extra_commands = os.path.abspath(extra_commands)
  scheduler = policy.get('general','scheduler2')
  if version.LooseVersion(phosim_version) < version.LooseVersion('3.1.0'):
    if len(sensor_ids.split('|')) > 1:
      logger.critical('Multiple sensors not supported in version < 3.1.0.')
      return 1
    sensor_id = '' if sensor_ids == 'all' else sensor_ids
    return DoPreprocOldVersion(trimfile, policy, extra_commandsm,scheduler,
                               sensor_id)
  elif version.LooseVersion(phosim_version) > version.LooseVersion('3.2.0'):
    if sensor_ids != 'all':
      logger.critical('Single exposure mode is currently not supported for'
                       ' phosim > 3.2.0')
      return 1
    return DoPreproc(trimfile, imsim_config_file, extra_commands, scheduler,
                     skip_atmoscreens=skip_atmoscreens,
                     keep_scratch_dirs=keep_scratch_dirs)
  logger.critical('Unsupported phosim version %s', phosim_version)
  return 1

if __name__ == '__main__':

  usage = 'usage: %prog trimfile imsim_config_file [options]'
  parser = OptionParser(usage=usage)
  parser.add_option('-a', '--skip_atmoscreens', dest='skip_atmoscreens',
                    action='store_true', default=False,
                    help='Generate atmospheric screens in raytrace stage instead'
                    ' of preprocessing stage.')
  parser.add_option('-c', '--command', dest='extra_commands',
                    help='Extra commands filename.')
  parser.add_option('-k', '--keep_scratch', dest='keep_scratch_dirs',
                    action='store_true', default=False,
                    help='Do not cleanup working directories.'
                    ' (version 3.2.x and higher only).')
  parser.add_option('-l', '--logtostdout', dest='log_to_stdout',
                    action='store_true', default=False,
                    help='Write logging output to stdout instead of log file'
                    ' (version 3.2.x and higher only).')
  parser.add_option('-s', '--sensor', dest='sensor_ids', default='all',
                    help='Specify a list of sensor ids to use delimited by "|",'
                    ' or use "all" for all.')
  (options, args) = parser.parse_args()
  if len(args) != 2:
    print 'Incorrect number of arguments.  Use -h or --help for help.'
    print usage
    quit()

  trimfile = args[0]
  imsim_config_file = args[1]
  sys.exit(main(trimfile, imsim_config_file, options.extra_commands,
                options.skip_atmoscreens, options.keep_scratch_dirs,
                options.sensor_ids, options.log_to_stdout))
