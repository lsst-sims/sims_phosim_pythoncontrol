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
import logging
import sys
from distutils import version
from AllChipsScriptGenerator import AllChipsScriptGenerator
from Focalplane import WithTimer  # TODO(gardnerj): Move this to PhosimUtil.
import PhosimManager

logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:  %(message)s',
                    filename='/tmp/fullFocalplane.log',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

def main(trimfile, imsim_config_file, extra_commands, id):

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
    with WithTimer() as t:
      # Determine the pre-processing scheduler so that we know which class to use
      if scheduler == 'csh':
        scriptGenerator = AllChipsScriptGenerator(trimfile, policy, extra_commands)
        scriptGenerator.makeScripts(id)
      elif scheduler == 'pbs':
        scriptGenerator = AllChipsScriptGenerator_Pbs(trimfile, policy, extra_commands)
        scriptGenerator.makeScripts(id)
      elif scheduler == 'exacycle':
        print 'Exacycle funtionality not added yet.'
        quit()
      else:
        print 'Scheduler "%s" unknown.  Use -h or --help for help.' % scheduler
    t.LogWall('makeScripts')
    return True
  elif version.LooseVersion(phosim_version) > version.LooseVersion('3.2.0'):
    if id:
      logging.critical('Single exposure mode is currently not supported for'
                       ' phosim > 3.2.0\n')
      return False
    if scheduler == 'csh':
      preprocessor = PhosimManager.PhosimPreprocessor(trimfile, policy, extra_commands)
    elif scheduler == 'pbs':
        logging.critical('PBS not supported yet.')
        return False
    else:
        logging.critical('Unknown scheduler: %s. Use -h or --help for help',
                         scheduler)
        return False
    preprocessor.InitExecEnvironment()
    with WithTimer() as t:
      if not preprocessor.DoPreprocessing(skip_atmoscreens=True):
        logging.critical('DoPreprocessing() failed.')
        return False
    t.LogWall('DoPreprocessing')
    archive_names = preprocessor.ArchiveOutput()
    if not archive_names:
      logging.critical('StageOutput() failed.')
      return False
    with WithTimer() as t:
      preprocessor.StageOutput(archive_names)
    t.LogWall('StageOutput')
    return True
  logging.critical('Unsupported phosim version %s', phosim_version)
  return False

if __name__ == '__main__':

  if len(sys.argv) < 4 or len(sys.argv) > 5:
      print ('usage: python fullFocalplane.py trimfile imsim_config_file'
             ' extraid_file/extra_commands [Rxx_Sxx_Exxx]')
      quit()

  trimfile = sys.argv[1]
  imsim_config_file = sys.argv[2]
  extra_commands = sys.argv[3]
  id = ''
  if len(sys.argv) == 5:
    id = sys.argv[4]

  main(trimfile, imsim_config_file, extra_commands, id)
