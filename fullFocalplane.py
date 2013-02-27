#!/usr/bin/python

"""
Brief:   Python script to create the 378 shell scripts necessary to execute
         the raytracing and subsequent portions of the ImSim workflow.
         (189 chips + 2 exposures per chip)
         In general, this is called by the script that was made by
         SingleVisitScriptGenerator.  All of the work is done by the
         AllChipsScriptGenerator class.  Similar to Nicole's original version,
         it can also be called with a single-chip exposure argument and will just
         work on a single chip and not generate any script.

         Note that which scheduler to use is determined by the 'scheduler2'
         option in the config file.

Date:    Jan 26, 2012
Authors: Nicole Silvestri, U. Washington, nms21@uw.edu,
         Jeff Gardner, U. Washington, Google, gardnerj@phys.washington.edu
Updated:

Usage:   python fullFocalplanePbs.py [options]
Options: trimfile:    absolute path and name of the trimfile
                      to process (unzipped)
         policy:      your copy of the imsimPbsPolicy.paf file
         extraidFile: name of the file containing extra parameters
                      to change simulator defaults (eg. turn clouds off)

         To run in single chip mode, supply the full exposure ID ('Rxx_Sxx_Exxx')
         as the optional 4th argument.  R=Raft, S=Sensor, E=Exposure

"""
from __future__ import with_statement
import ConfigParser
from distutils import version
import logging
from optparse import OptionParser  # Can't use argparse yet, since we must work in 2.5
import sys
from AllChipsScriptGenerator import AllChipsScriptGenerator
from Focalplane import WithTimer  # TODO(gardnerj): Move this to PhosimUtil.
import PhosimManager

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:  %(message)s',
                    filename='/tmp/fullFocalplane.log',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

def main(trimfile, imsim_config_file, extra_commands, skip_atmoscreens,
         keep_scratch_dirs, sensor_ids):

  """
  Run the fullFocalplanePbs.py script, populating it with the
  correct user and cluster job submission information from an LSST
  policy file.
  """

  # print 'Running fullFocalPlane on: ', trimfile
  logger.info('Running fullFocalPlane on: %s ', trimfile)

  # Parse the config file
  policy = ConfigParser.RawConfigParser()
  policy.read(imsim_config_file)
  if policy.has_option('general', 'phosim_version'):
    phosim_version = policy.get('general', 'phosim_version')
  else:
    phosim_version = '3.0.1'
  # print 'Using Imsim/Phosim version', phosim_version
  logger.info('Using Imsim/Phosim version %s', phosim_version)
  scheduler = policy.get('general','scheduler2')
  if version.LooseVersion(phosim_version) < version.LooseVersion('3.1.0'):
    if len(sensor_ids.split('|')) > 1:
      logging.critical('Multiple sensors not supported in version < 3.1.0.')
      return 1
    sensor_id = '' if sensor_ids == 'all' else sensor_ids
    with WithTimer() as t:
      # Determine the pre-processing scheduler so that we know which class to use
      if scheduler == 'csh':
        scriptGenerator = AllChipsScriptGenerator(trimfile, policy, extra_commands)
        scriptGenerator.makeScripts(sensor_id)
      elif scheduler == 'pbs':
        scriptGenerator = AllChipsScriptGenerator_Pbs(trimfile, policy, extra_commands)
        scriptGenerator.makeScripts(sensor_id)
      elif scheduler == 'exacycle':
        print 'Exacycle funtionality not added yet.'
        quit()
      else:
        print 'Scheduler "%s" unknown.  Use -h or --help for help.' % scheduler
    t.LogWall('makeScripts')
    return 0
  elif version.LooseVersion(phosim_version) > version.LooseVersion('3.2.0'):
    if sensor_ids != 'all':
      logging.critical('Single exposure mode is currently not supported for'
                       ' phosim > 3.2.0')
      return 1
    if scheduler == 'csh':
      preprocessor = PhosimManager.PhosimPreprocessor(trimfile, policy, extra_commands)
    elif scheduler == 'pbs':
        logging.critical('PBS not supported yet.')
        return 1
    else:
        logging.critical('Unknown scheduler: %s. Use -h or --help for help',
                         scheduler)
        return 1
    preprocessor.InitExecEnvironment()
    with WithTimer() as t:
      if not preprocessor.DoPreprocessing(skip_atmoscreens=skip_atmoscreens):
        logging.critical('DoPreprocessing() failed.')
        return 1
    t.LogWall('DoPreprocessing')
    archive_names = preprocessor.ArchiveRaytraceInputByExt(skip_atmoscreens=skip_atmoscreens)
    archive_names.extend(preprocessor.ArchiveRaytraceScriptsByExt())
    if not archive_names:
      logging.critical('Output archive step failed.')
      return 1
    with WithTimer() as t:
      preprocessor.StageOutput(archive_names)
    t.LogWall('StageOutput')
    if not keep_scratch_dirs:
      preprocessor.Cleanup()
    return 0
  logging.critical('Unsupported phosim version %s', phosim_version)
  return 1

if __name__ == '__main__':

  usage = 'usage: %prog [options] trimfile imsim_config_file'
  parser = OptionParser(usage=usage)
  parser.add_option('-c', '--command', dest='extra_commands',
                    help='Extra commands filename.')
  parser.add_option('-s', '--sensor', dest='sensor_ids', default='all',
                    help='Specify a list of sensor ids to use delimited by "|",'
                    ' or use "all" for all.')
  parser.add_option('-a', '--skip_atmoscreens', dest='skip_atmoscreens',
                    action='store_true', default=False,
                    help='Generate atmospheric screens in raytrace stage instead'
                    ' of preprocessing stage.')
  parser.add_option('-k', '--keep_scratch', dest='keep_scratch_dirs',
                    action='store_true', default=False,
                    help='Do not cleanup working directories.')
  (options, args) = parser.parse_args()
  if len(args) != 2:
    print 'Incorrect number of arguments.  Use -h or --help for help.'
    print usage
    quit()

  trimfile = args[0]
  imsim_config_file = args[1]
  sys.exit(main(trimfile, imsim_config_file, options.extra_commands,
                options.skip_atmoscreens, options.keep_scratch_dirs,
                options.sensor_ids))
