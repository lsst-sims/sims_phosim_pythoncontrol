#!/usr/bin/python

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

Notes:   The script takes a list of trimfiles to run.
         The extraidFile is the name of an additional file used to change the
         default imsim parameters (eg. to turn clouds off, create centroid files, etc).

Methods: 1. header: write the PBS script header info for the run
         2. logging: write some useful info for the log files
         3. setupCleanup: setup and launch the cleanup script.
            The cleanup script will cleanup crashed jobs on the node.
         4. writeJobCommands: add code command lines to the PBS script
         5. cleanNodeDir: Copy/move all data to stagePath2 and delete node
            directory and all remaining data.
         6. makePbsScripts: This is the main module. It makes the
            individual instance catalog job PBS
            scripts and tars up data for the node job.  The PBS scripts
            and tar file are saved to the directory you run the script in.

To Do:   Remove directory structure dependence from all imsim code.
         Include verifyData tests for data dir integrity.

"""

from __future__ import with_statement
import os, re, sys
import random


class AbstractScriptGenerator:
    """
    This is an abstract class that has methods that are identical for the preprocessing
    and raytracing stages (implemented by 'SingleVisitScriptGenerator' and
    'SingleChipScriptGenerator' respectively).

    When implemented, this class has a single external method, called makeScript().
    makeScript() calls at least 6 internal methods in the following order:
           - writeHeader            Write script header
           - writeSetupExecDirs     Write commands to setup the directories on exec node
           - writeCopySharedData    Write commands to copy the shared data tarball to exec node
           - writeCopyStagedFiles   Write commands to copy staged data to exec node
           - writeJobCommands       Write the actual execution commands
           - writeCleanupCommands   Write the commands to cleanup
    3 of these are general enough to be provided in this abstract class:
      writeSetupExecDirs(), writeCopySharedData(), and writeCleanupCommands().
    """

    def writeSetupExecDirs(self, scriptFileName, visitDir):
        """
        Create directories on exec node.
        """
        try:
            pbsout = open(scriptFileName, 'a')
        except IOError:
            print "Could not open %s for writing cleanup script info for PBS script" %(pbsfilename)
            sys.exit()

        visitPath = os.path.join(self.scratchPath, visitDir)

        try:
            with file(scriptFileName, 'a') as cshOut:
                print >>cshOut, " "
                print >>cshOut, "### ---------------------------------------"
                print >>cshOut, "### Set up exec node directories"
                print >>cshOut, "### ---------------------------------------"
                print >>cshOut, " "
                print >>cshOut, "## create local node directories (visitPath = %s)" %(visitPath)
                # check if directory already exists.  If not, then try creating it.  If it cannot
                # be created, then maybe we are not on an exec node.
                print >>cshOut, "if (! -d %s) then" %(self.scratchPath)
                print >>cshOut, "  mkdir -p %s" %(self.scratchPath)
                print >>cshOut, "endif"
                print >>cshOut, "if (! -d %s) then" %(self.scratchPath)
                print >>cshOut, "  echo 'Directory %s could not be created.'" %(self.scratchPath)
                print >>cshOut, "  echo 'Are you sure you are on a compute node?'; exit 1"
                print >>cshOut, "endif"
                print >>cshOut, "if (! -d %s) then" %(visitPath) # see if directory exists
                print >>cshOut, "  mkdir -p %s" %(visitPath)  # make the directory (including parents)
                print >>cshOut, "endif"
                print >>cshOut, "if (! -d %s) then" %(visitPath) # check if directory creation worked
                print >>cshOut, "  echo 'Something failed in creating local directory %s. Exiting.'" %(visitPath)
                print >>cshOut, "  exit 1"
                print >>cshOut, "endif"
        except IOError:
            print "Could not open %s for writing shell script" %(scriptFileName)
            sys.exit()
        return



    def _writeCopyDataTarball(self, cshOut, dataPath, dataTarball,
                              scratchSharedPath, dataCheckDir):
        """Generic method for copying and untarring a data tarball to
           the compute node.
        Inputs:
           cshOut:            Pointer to output script
           dataPath:          Path to the data tarball
           dataTarBall:       Name of data tarball
           scratchSharedPath: Directory into which to untar the tarball
           dataCheckDir:      If this directory exists inside scratchSharedPath,
                              do no bother staging tarball.
        TODO(gardner): Use different lock files for FP vs. SED?
        """
        # Make sure your shared directory on the compute node exists
        cshOut.write('if ( ! -d %s ) then \n' %(scratchSharedPath))
        cshOut.write('  mkdir -p %s \n' %(scratchSharedPath))
        cshOut.write('endif \n')
        cshOut.write('cd %s \n' %(scratchSharedPath))
        # Make sure the data directory and all files are present on the exec node.
        cshOut.write('echo Initializing lock file. \n')
        cshOut.write('lockfile -l 1800 imsim_shared_data.lock \n')
        cshOut.write('if (-d %s ) then \n' %dataCheckDir)
        cshOut.write('  echo Good news everyone! The data directory %s already exists! \n'
                     %os.path.join(scratchSharedPath, dataCheckDir))
        cshOut.write('else \n')
        cshOut.write('  echo The shared data directory %s does not exist. Copying %s. \n'
                     %(os.path.join(scratchSharedPath, dataCheckDir),
                       os.path.join(dataPath, dataTarball)))
        cshOut.write('  cp %s . \n' %(os.path.join(dataPath, dataTarball)))
        cshOut.write('  echo Untarring %s \n' %(dataTarball))
        cshOut.write('  tar xf %s \n' %(dataTarball))
        cshOut.write('  rm %s \n' %(dataTarball))
        cshOut.write('  echo Finished copying shared data to exec node.\n')
        cshOut.write('endif \n')
        cshOut.write('rm -f imsim_shared_data.lock \n')
        cshOut.write('echo Removed lock file and copying files for the node. \n')


    def writeSetupSharedData(self, scriptFileName, wuID, needFP=False, needSEDs=False):
        """Writes commands to the script for setting up the 'data' directory
           within the exec directory.  Whether it stages a tarball or symbolically
           links to a shared directory is determed by the self.policy.
        Args:
           scriptFileName:   Name of the script being written to
           wuID:             ID of the work unit, constructed as:
                                wuID = '%s-f%s-%s' %(obshistid, filterName, id)
                             where 'id' is of the form:
                                id = 'R'+rx+ry+'_'+'S'+sx+sy+'_'+'E00'+ex
           needFP:           'True' if focal_plane data is required
           needSEDs:         'True' if SEDs are required
        """
        wuPath = os.path.join(self.scratchPath, wuID)
        if self.sleepMax > 0:
            myRandInt = random.randint(0,self.sleepMax)
        else:
            myRandInt = 0

        try:
            with file(scriptFileName, 'a') as cshOut:
                print >>cshOut, " "
                print >>cshOut, "### ---------------------------------------"
                print >>cshOut, "### Copy shared data to exec node"
                print >>cshOut, "### ---------------------------------------"
                print >>cshOut, " "
                cshOut.write('echo Sleeping for %s seconds. \n' %(myRandInt))
                cshOut.write('sleep %s \n' %(myRandInt))
                execDataPath = os.path.join(wuPath, 'data')
                cshOut.write('mkdir %s\n' %execDataPath)
                cshOut.write('cd %s\n' %execDataPath)
                if needFP:
                    if self.policy.getboolean('general', 'useSharedFP') == False:
                        scratchDataPath = self.policy.get('general','scratchDataPath')
                        # writeCopyDataTarball() will check the existence of
                        # 'focal_plane/sta_misalignments/qe_maps' to determine if it
                        # needs to grab and untar self.tarball.
                        self._writeCopyDataTarball(cshOut,
                                                   self.policy.get('general','dataPathFP'),
                                                   self.policy.get('general','dataTarballFP'),
                                                   scratchDataPath,
                                                   'focal_plane/sta_misalignments/qe_maps')
                    else:
                        scratchDataPath = self.policy.get('general','dataPathFP')
                    cshOut.write('cd %s\n' %execDataPath)
                    cshOut.write('echo Setting soft link to focal_plane directory. \n')
                    cshOut.write('ln -s %s focal_plane\n' %os.path.join(scratchDataPath,
                                                                        'focal_plane'))
                if needSEDs:
                    if self.policy.getboolean('general', 'useSharedSEDs') == False:
                        scratchDataPath = self.policy.get('general','scratchDataPath')
                        # writeCopyDataTarball() will check the existence of
                        # 'starSED/gizisSED' to determine if it
                        # needs to grab and untar self.tarball.
                        self._writeCopyDataTarball(cshOut,
                                                   self.policy.get('general','dataPathSEDs'),
                                                   self.policy.get('general','dataTarballSEDs'),
                                                   scratchDataPath,
                                                   'starSED/gizis_SED')
                    else:
                        scratchDataPath = self.policy.get('general','dataPathSEDs')
                    cshOut.write('cd %s\n' %execDataPath)
                    cshOut.write('echo Setting soft link to SED data directories. \n')
                    cshOut.write('ln -s %s agnSED\n' %os.path.join(scratchDataPath,
                                                                 'agnSED'))
                    cshOut.write('ln -s %s flatSED\n' %os.path.join(scratchDataPath,
                                                                 'flatSED'))
                    cshOut.write('ln -s %s galaxySED\n' %os.path.join(scratchDataPath,
                                                                 'galaxySED'))
                    cshOut.write('ln -s %s ssmSED\n' %os.path.join(scratchDataPath,
                                                                 'ssmSED'))
                    cshOut.write('ln -s %s starSED\n' %os.path.join(scratchDataPath,
                                                                 'starSED'))
        except IOError:
            print "Could not open %s for writing shell script" %(scriptFileName)
            sys.exit()
        return

    def writeCleanupCommands(self, scriptFileName, visitDir):
        """
        Remove directories on exec node.

        """
        visitPath = os.path.join(self.scratchPath, visitDir)

        try:
            with file(scriptFileName, 'a') as cshOut:
                print >>cshOut, "\n### -------------------------------------------------"
                print >>cshOut, "### Remove the visit-specific directory on exec node."
                print >>cshOut, "### (does not delete parent directories if created)"
                print >>cshOut, "### -------------------------------------------------\n"
                print >>cshOut, "echo Now deleting files in %s" %(visitPath)
                print >>cshOut, "/bin/rm -rf %s" %(visitPath)
                print >>cshOut, "echo ---"
                print >>cshOut, "echo Job finished at `date`"
                print >>cshOut, " "
                print >>cshOut, "###"
        except IOError:
            print "Could not open %s for writing cleanup commands." %(scriptFileName)
            sys.exit()

        return
