#!/share/apps/lsst_gcc440/Linux64/external/python/2.5.2/bin/python

"""
Brief:   Python script to generate a PBS file for each instance
         catalog (trimfile) - one pbs script per visit.  Upon submission, each
         of these scripts will be run individually on a cluster node to create
         the necessary *.pars, *.fits and *.pbs files for individual sensor
         jobs.
         
Usage:   python generateVisitPbs.py [options]
Options: fileName: Name of file containing trimfile list
         imsimPolicyFile: Name of your policy file
         extraidFile: Name of the extraidFile to include

Date:    November 30, 2011
Authors: Nicole Silvestri, U. Washington, nms@astro.washington.edu
         Jeffrey P. Gardner, U. Washington, Google, gardnerj@phys.washington.edu
Updated: November 30, 2011 JPG: Removed dependency on LSST stack by swapping
                                LSST policy file with Python ConfigParser
         December 21, 2011 JPG: Objectified code and moved objects to ScriptGenerator.py

Notes:   The script takes a list of trimfiles to run.  
         The extraidFile is the name of an additional file used to change the
         default imsim parameters (eg. to turn clouds off, create centroid files, etc).

"""

from ScriptGenerator import *

if __name__ == "__main__":

    if not len(sys.argv) == 4:
        print "usage: python generateVisitPbs.py trimfileName imsimConfigFile extraidFile"
        quit()

    myfile = sys.argv[1]
    imsimConfigFile = sys.argv[2]
    extraIdFile = sys.argv[3]

    scriptGenerator = AllVisitsPbsGenerator(myfile, imsimConfigFile, extraIdFile)
    scriptGenerator.makeScripts()
    #makePbsScripts(myfile, imsimConfigFile, extraidFile)
