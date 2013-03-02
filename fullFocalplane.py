#!/usr/bin/python

"""
ADD DOCUMENTATION!
TODO(gardnerj): Add stdout log redirect
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

logger = logging.getLogger(__name__)

def DoPreprocOldVersion(trimfile, policy, extra_commands, scheduler, sensor_id):
  """Do preprocessing for v3.1.0 and earlier."""
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
  """Do preprocessing for v3.2.0 and later."""
  policy = ConfigParser.RawConfigParser()
  policy.read(imsim_config_file)
  if scheduler == 'csh':
    preprocessor = PhosimManager.PhosimPreprocessor(policy, imsim_config_file,
                                                    trimfile, extra_commands)
  elif scheduler == 'pbs':
      logger.critical('PBS not supported yet.')
      return 1
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
  return 0

def ConfigureLogging(trimfile, policy, log_to_stdout, imsim_config_file,
                     extra_commands=None):
  if log_to_stdout:
    log_fn = None
  else:
    if policy.has_option('general', 'log_dir'):
      # Log to file in log_dir
      obsid = PhosimManager.ObservationIdFromTrimfile(
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
                    help='Do not cleanup working directories.')
  parser.add_option('-l', '--logtostdout', dest='log_to_stdout',
                    action='store_true', default=False,
                    help='Write logging output to stdout instead of log file.')
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
