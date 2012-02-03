#!/usr/bin/python
################!/share/apps/lsst_gcc440/Linux64/external/python/2.5.2/bin/python

"""
!!!!!!!
NOTICE: 1/06/2012: 
!!!!!!!  Python control scripts have been moved from the main imsim branch
         to /sims/control/python.  For working with later revisions of ImSim,
         please work with the separate python control branch.

         Also note: use the newer .cfg files and not the .paf.

         These python scripts work *only* with trunk tag v-2.2.1

Brief:   A Python script to generate a pre-processing script for each instance
         catalog (trimfile) - one script per visit. When executing, the
         pre-processing script will in turn generate 189 scripts for raytracing,
         one per detector.  The flavor of each script (i.e. what scheduling
         environment it is designed for) can be varied per-phase by setting
         the 'scheduler1' and 'scheduler2' options.

Usage:   python generateVisit.py [options]
Options: trimfileListName: Name of file containing trimfile list
         imsimConfigFile: Name of your config file (note: no longer uses LSST policy format)
         extraidFile: Name of the extraidFile to include

Date:    November 30, 2011
Authors: Nicole Silvestri, U. Washington, nms@astro.washington.edu
         Jeffrey P. Gardner, U. Washington, Google, gardnerj@phys.washington.edu
Updated: 
         November 30, 2011 JPG: Removed dependency on LSST stack by swapping
                                LSST policy file with Python ConfigParser
         December 21, 2011 JPG: Objectified code and moved objects to ScriptGenerator.py
         January 6, 2012  JPG: Objectification of generateVisit (which uses
                AllVisistsScriptGenerator) complete.  Reworked configuration parameters so
                directory names make more sense.  Moved objects out of here and into
                to their proper files.
         02/01/2012 JPG: No longer takes the scheduler as an argument but reads it from
                         config file

Notes:   The script takes a list of trimfiles to run.
         The extraidFile is the name of an additional file used to change the
         default imsim parameters (eg. to turn clouds off, create centroid files, etc).

"""

from AllVisitsScriptGenerator import *
from optparse import OptionParser

if __name__ == "__main__":


    usage = "usage: %prog [options] trimfileListName imsimConfigFile extraidFile"
    parser = OptionParser(usage=usage)
    parser.add_option("-s", "--scheduler", dest="scheduler", default="unspecified",
                      help="depricated")
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

    if options.scheduler != "unspecified":
        print "--scheduler command-line option no longer supported.  Use 'scheduler1' and 'scheduler2"
        print "  in imsimConfigFile."
        quit()
    # Parse the config file
    policy = ConfigParser.RawConfigParser()
    policy.read(imsimConfigFile)
    # Determine the pre-processing scheduler so that we know which class to use
    scheduler = policy.get('general','scheduler1')
    if scheduler == 'csh':
        scriptGenerator = AllVisitsScriptGenerator(myfile, policy, imsimConfigFile, extraIdFile)
        scriptGenerator.makeScripts()
    elif scheduler == 'pbs':
        scriptGenerator = AllVisitsScriptGenerator_Pbs(myfile, policy, imsimConfigFile, extraIdFile)
        scriptGenerator.makeScripts()
    elif scheduler == 'exacycle':
        print "Exacycle funtionality not added yet."
        quit()
    else:
        print "Scheduler '%s' unknown.  Use -h or --help for help." %(scheduler)
