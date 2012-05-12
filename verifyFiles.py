#!/usr/bin/python

"""
Brief:   Python script to verify file as various stages of the ImSim workflow.

Date:    May 11, 2012
Author:  Jeff Gardner, U. Washington, gardnerj@phys.washington.edu
Updated:

Usage:   python verifyFiles.py [options] obshistid filter path
Options: -s, --stage:  workflow stage to check:
                         input: input files for raytrace
                         exec_output: raytrace output files on exec node
                         final_output: raytrace output files in shared storage
         obshistid:    obshistid
         filter:       letter filter ID
         path:         Path to the data, excluding any <obshistid>-f<filter> info

Example: To verify input files for obshistid=871731611, filter=z, stagePath2=/share/gardnerj/staging2:
             python verifyFiles.py --stage input 871731611 z /share/gardnerj/staging2
         (The files in question are actually in /share/gardnerj/staging2/871731611-fz/run871731611)
"""

from __future__ import with_statement
import sys
import optparse
from Focalplane import *
from Exposure import *
from chip import WithTimer

def main(stage, obshistid, filterid, path, idList, expList, camstr):
    missingList = []
    corruptList = []
    fp = Focalplane(obshistid, filterid)
    if stage == 'raytrace_input':
        missingList = fp.verifyInputFiles(path, idList)
    elif stage == 'raytrace_exec':
        if not idList or not len(idList)==1:
            raise RuntimeError("IDLIST must be set to exactly 1 id in raytrace_exec mode")
        exposure = Exposure(obshistid, filterid, idList[0])
        missingList, corruptList = exposure.verifyExecFiles(path)
    elif stage == 'raytrace_output':
        if not idList:
            for cidTuple in fp.generateCidList(camstr, idList):
                for expid in expList:
                    idList.append('%s_%s' %(cidTuple[0], expid))
        for id in idList:
            exposure = Exposure(obshistid, filterid, id)
            missingList.extend(exposure.verifyOutputFiles(path))
    else:
        sys.stderr.write('Unrecognized stage: %s\n' %stage)
        return 1
    returncode = 0
    if len(missingList) > 0 or len(corruptList) > 0:
        sys.stderr.write('WARNING: verifyFiles returned errors:\n')
    if len(missingList) > 0:
        for f in missingList:
            sys.stderr.write('-- FileNotFound: %s\n' %f)
        returncode += 2
    if len(corruptList) > 0:
        for f,e in corruptList:
            sys.stderr.write('-- FitsFileCorrupt: %s\n' %e)
        returncode += 4
    return returncode


if __name__ == "__main__":

    usage = "usage: %prog [options] obshistid filter path"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-s", "--stage", dest="stage",
                      help="Stage to check: raytrace_input, raytrace_exec, raytrace_output (default)",
                      default="final_output")
    parser.add_option("-i", "--idlist", dest="idlist", default="",
                      help="(optional) comma-separated list of ids (Rxx_Sxx_Exxx) to process." +
                           "  Overrides EXPLIST.")
    parser.add_option("-e", "--explist", dest="explist", default="E000,E001",
                      help="If IDLIST not specified, a comma-separated list of exposures to process" +
                           " in stages 'raytrace_exec' and 'raytrace_output' ('raytrace_input' will determine this" +
                           " from the exec_* files)")
    parser.add_option("-c", "--camstr", dest="camstr", default="Group0",
                      help="If IDLIST not specified, a regex string for which camera group to use" +
                           " (see fullFocalPlane script and SIM_CAMCONFIG for more details)")
    (options, args) = parser.parse_args()

    if len(args) != 3:
        parser.print_help()
        sys.exit(1)
    obshistid = args[0]
    filterid = args[1]
    path = args[2]
    idlist = []
    if options.idlist:
        idlist = options.idlist.split(",")
    if not options.explist:
        raise RuntimeError("Unrecognized explist")
    explist = options.explist.split(",")
    result = main(options.stage, obshistid, filterid, path, idlist, explist, options.camstr)
    sys.exit(result)
        
