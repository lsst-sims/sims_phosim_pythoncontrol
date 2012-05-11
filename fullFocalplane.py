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
import sys
import ConfigParser
from AllChipsScriptGenerator import *
from chip import WithTimer

def main(trimfile, imsimConfigFile, extraidFile, id):

    """
    Run the fullFocalplanePbs.py script, populating it with the
    correct user and cluster job submission information from an LSST
    policy file.
    """

    print 'Running fullFocalPlane (and AllChipsScriptGenerator) on: ', trimfile

    # Parse the config file
    policy = ConfigParser.RawConfigParser()
    policy.read(imsimConfigFile)
    # Determine the pre-processing scheduler so that we know which class to use
    scheduler = policy.get('general','scheduler2')
    with WithTimer() as t:
        if scheduler == 'csh':
            scriptGenerator = AllChipsScriptGenerator(trimfile, policy, extraidFile, id)
            scriptGenerator.makeScripts()
        elif scheduler == 'pbs':
            scriptGenerator = AllChipsScriptGenerator_Pbs(trimfile, policy, extraidFile, id)
            scriptGenerator.makeScripts()
        elif scheduler == 'exacycle':
            print "Exacycle funtionality not added yet."
            quit()
        else:
            print "Scheduler '%s' unknown.  Use -h or --help for help." %(scheduler)
    t.PrintWall('fullFocalplane.py', sys.stderr)


if __name__ == "__main__":

    if len(sys.argv) < 4 or len(sys.argv) > 5:
        print "usage: python fullFocalplane.py trimfile imsimConfigFile extraidFile [Rxx_Sxx_Exxx]"
        quit()

    trimfile = sys.argv[1]
    imsimConfigFile = sys.argv[2]
    extraidFile = sys.argv[3]
    id = ""
    if len(sys.argv) == 5:
      id = sys.argv[4]

    main(trimfile, imsimConfigFile, extraidFile, id)
