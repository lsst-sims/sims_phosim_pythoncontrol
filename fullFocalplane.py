#!/share/apps/lsst_gcc440/Linux64/external/python/2.5.2/bin/python
############!/usr/bin/env python

"""
Brief:   Python script to create the 378 shell scripts necessary to execute
         the raytracing and subsequent portions of the ImSim workflow.
         (189 chips + 2 exposures per chip)
         In general, this is called by the script that was made by
         SingleVisitScriptGenerator.  All of the work is done by the
         AllChipsScriptGenerator class.  Like Nicole's original version,
         it can also be called with rx,ry,sx,sy,ex arguments and will just
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

         If running in single chip mode, you will also need the following options:
         rx: Raft x value 
         ry: Raft y value
         sx: Sensor x value
         sy: Sensor y value
         ex: Snap x value

"""
import ConfigParser
from AllChipsScriptGenerator import *
#import lsst.pex.policy as pexPolicy
#import lsst.pex.logging as pexLog
#import lsst.pex.exceptions as pexExcept



def main(trimfile, imsimConfigFile, extraidFile, rx, ry, sx, sy, ex):

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
    if scheduler == 'shell':
        scriptGenerator = AllChipsScriptGenerator(trimfile, policy, extraidFile, rx, ry, sx, sy, ex)
        scriptGenerator.makeScripts()
    elif scheduler == 'pbs':
        scriptGenerator = AllChipsScriptGenerator_Pbs(trimfile, policy, extraidFile, rx, ry, sx, sy, ex)
        scriptGenerator.makeScripts()
    elif scheduler == 'exacycle':
        print "Exacycle funtionality not added yet."
        quit()
    
    
if __name__ == "__main__":

    if not len(sys.argv) == 9:
        print "usage: python fullFocalplane.py trimfile imsimConfigFile extraidFile rx ry sx sy ex"
        quit()

    trimfile = sys.argv[1]
    imsimConfigFile = sys.argv[2]
    extraidFile = sys.argv[3]
    rx = sys.argv[4]
    ry = sys.argv[5]
    sx = sys.argv[6]
    sy = sys.argv[7]
    ex = sys.argv[8]

    main(trimfile, imsimConfigFile, extraidFile, rx, ry, sx, sy, ex)
