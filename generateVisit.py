#!/share/apps/lsst_gcc440/Linux64/external/python/2.5.2/bin/python

"""
!!!!!!!
NOTICE: 1/06/2012: These python scripts work *only* with trunk revision 23580!
!!!!!!!  Python control scripts have been moved from the main imsim branch
         to /sims/control/python.  For working with later revisions of ImSim,
         please work with the separate python control branch.

         Also note: use the newer .cfg files and not the .paf.

Brief:   A Python script to generate a script for each instance
         catalog (trimfile) - one script per visit.  This will accept multiple
         types of scripts (e.g. shell, PBS, exacycle). Upon submission, each
         of these scripts will be run individually on a cluster node to create
         the necessary *.pars, *.fits and *.[pbs,csh,py] files for individual sensor
         jobs.
         
Usage:   python generateVisit.py [options]
Options: fileName: Name of file containing trimfile list
         imsimPolicyFile: Name of your policy file
         extraidFile: Name of the extraidFile to include

Date:    November 30, 2011
Authors: Nicole Silvestri, U. Washington, nms@astro.washington.edu
         Jeffrey P. Gardner, U. Washington, Google, gardnerj@phys.washington.edu
Updated: November 30, 2011 JPG: Removed dependency on LSST stack by swapping
                                LSST policy file with Python ConfigParser
         December 21, 2011 JPG: Objectified code and moved objects to ScriptGenerator.py
         January 6, 2012  JPG: Objectification of generateVisit (which uses
                ScriptGenerator) complete.  Reworked configuration parameters so
                directory names make more sense.

Notes:   The script takes a list of trimfiles to run.  
         The extraidFile is the name of an additional file used to change the
         default imsim parameters (eg. to turn clouds off, create centroid files, etc).

"""

from AllVisitsScriptGenerator import *
from optparse import OptionParser

if __name__ == "__main__":


    usage = "usage: %prog [options] trimfileName imsimConfigFile extraidFile"
    parser = OptionParser(usage=usage)
    parser.add_option("-s", "--scheduler", dest="scheduler", default="shell",
                      help="Specify SCHEDULER type: shell, pbs, exacycle (default=shell)")
    #parser.add_option("-v", "--verbose", type="int", dest="verbosity", default=0,
    #                  help="Level of verbosity. >0 means shell scripts are run with '-x' (default=0)")
    (options, args) = parser.parse_args()
    if len(args) != 3:
        print "Incorrect number of arguments.  Use -h or --help for help."
        quit()

    myfile = args[0]
    imsimConfigFile = args[1]
    extraIdFile = args[2]

    print "Called with myfile=%s, imsimConfigFile=%s, extraIdFile=%s" %(myfile, imsimConfigFile, extraIdFile)

    if options.scheduler == 'shell':
        scriptGenerator = AllVisitsScriptGenerator(myfile, imsimConfigFile, extraIdFile)
        scriptGenerator.makeScripts()
    elif options.scheduler == 'pbs':
        scriptGenerator = AllVisitsPbsGenerator(myfile, imsimConfigFile, extraIdFile)
        scriptGenerator.makeScripts()
    elif options.scheduler == 'exacycle':
        print "Exacycle funtionality not added yet."
        quit()
    else:
        print "%s is an invalid scheduler option" %(options.scheduler)
