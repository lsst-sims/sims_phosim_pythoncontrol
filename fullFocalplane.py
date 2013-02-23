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
import logger
import sys
from distutils import version
from AllChipsScriptGenerator import *
from chip import WithTimer

logger = logging.getLogger(__name__)
logging.basicConfig(filename='/tmp/fullFocalplane.log')

def main(trimfile, imsimConfigFile, extraidFile, id):

    """
    Run the fullFocalplanePbs.py script, populating it with the
    correct user and cluster job submission information from an LSST
    policy file.
    """

    print 'Running fullFocalPlane on: ', trimfile
    logging.info('Running fullFocalPlane on: %s ', trimfile

    # Parse the config file
    policy = ConfigParser.RawConfigParser()
    policy.read(imsimConfigFile)
    if policy.has_option('general', 'version'):
      phosim_version = policy.get('general', 'version')
    else:
      phosim_version = '3.0.1'
    scheduler = policy.get('general','scheduler2')
    if version.LooseVersion(phosim_version) < version.LooseVersion("3.2.0"):
      with WithTimer() as t:
        # Determine the pre-processing scheduler so that we know which class to use
        if scheduler == 'csh':
          scriptGenerator = AllChipsScriptGenerator(trimfile, policy, extraidFile)
          scriptGenerator.makeScripts(id)
        elif scheduler == 'pbs':
          scriptGenerator = AllChipsScriptGenerator_Pbs(trimfile, policy, extraidFile)
          scriptGenerator.makeScripts(id)
        elif scheduler == 'exacycle':
          print "Exacycle funtionality not added yet."
          quit()
        else:
          print "Scheduler '%s' unknown.  Use -h or --help for help." %(scheduler)
      t.LogWall('makeScripts')
      result = True
    else:
      if id:
        logging.critical('Single exposure mode is currently not supported for phosim > 3.2.0\n')
        sys.exit(-1)
      if scheduler == 'csh':
        preprocessor = PhosimPreprocessor(trimfile, policy, extraCommand)
      elif scheduler == 'pbs':
          logging.critical('PBS not supported yet.')
          sys.exit(-1)
      else:
          logging.critical('Unknown scheduler: %s. Use -h or --help for help', scheduler)
          sys.exit(-1)
      with WithTimer() as t:
        if not preprocessor.DoPreprocessing():
          logging.critical('DoPreprocessing() failed.')
          sys.exit(-1)
      t.LogWall('DoPreprocessing')
      preprocessor.ArchivePreprocOutput():
      with WithTimer() as t:
        if not preprocessor.StagePreprocOutput():
          logging.critical('StagePreprocOutput() failed.')
          sys.exit(-1)
      t.LogWall('StagePreprocOutput')
    return result


if __name__ == "__main__":

    if len(sys.argv) < 4 or len(sys.argv) > 5:
        print "usage: python fullFocalplane.py trimfile imsimConfigFile extraidFile/extraCommands [Rxx_Sxx_Exxx]"
        quit()

    trimfile = sys.argv[1]
    imsimConfigFile = sys.argv[2]
    extraidFile = sys.argv[3]
    id = ""
    if len(sys.argv) == 5:
      id = sys.argv[4]

    main(trimfile, imsimConfigFile, extraidFile, id)
