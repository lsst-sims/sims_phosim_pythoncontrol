==========================
Python ImSim Control Files
==========================

These files are for use with the LSST Image Simulator (ImSim).  As of
tag v-1.1, the Python scripts no longer need to be in the ImSim source
tree when they are executed.

For ImSim revisions predating and including 25503, these Python
control scripts are still part of the ImSim tree itself (after that,
they were moved over here).  The v-1.1 Python scripts and above will
ignore any Python scripts in the ImSim root, so you should not have to
worry about deleting them.

The correct procedure is to:
  1) Check out proper revision of ImSim (see below)
  2) Check out a compatible revision of the Python control package
  3) Execute from the directory containing the Python control package

==========================
REQUIREMENTS
==========================
1. The proper revision of ImSim (see "REVISIONS" below)
2. The "fitsverify" executable from the package
   http://heasarc.gsfc.nasa.gov/docs/software/ftools/fitsverify/
   must be in your path for the raytracing stage (see below).
3. Python 2.5 or later

==========================
REVISIONS
==========================

The following tags of the Python control package work with the
following revisions of ImSim:

Tag	  ImSim Rev    ImSim Tag  Notes:
------	  ---------    ---------  --------------------------------------------
v-1.0	  23580	       none	  Compatable w/ Nicole's documented version
				  except that it uses .cfg file and
				  not .paf.  In this version, the
				  Python control files must be run
				  from the root of the ImSim source tree.


v-1.1     25315        v-2.2.1    Still compatable with Nicole's documented
				  version (except for using .cfg files)
				  but works with ImSim tag v-2.2.1 *and*
				  Python files can reside in a separate
				  directory.

v-2.0     25583        v-2.2.1    Major rewrite.  No longer compatable
                                  with Nicole's version.  Reorg of
                                  config params to better accomodate
                                  Exacycle.

v-2.1                  v-2.2.1    Reorganization in the handling of
                mirrors/exacycle  the shared data (QE, height maps, SEDs)
		                  to be more flexible and accomodate
				  shared storage architectures.

v-2.2 		       v-2.2.1    Addition of unit tests
		mirrors/exacycle

v-2.2.1                v-2.2.1    Minor tweaks and bug fixes.  Last
                mirrors/exacycle  tag to work with ImSim v-2.2.1

v-3.0.1                v-3.0.1    Updated to work with ImSim v-3.0.1.
				  Lots of updates, including the
				  addition of timers for all major
				  workflow components, and the
				  "verifyFiles.py" script, which is
				  documented below.


==========================
USAGE
==========================

These scripts divide execution of the ImSim workflow into two distict
phases: "preprocessing" and "raytrace".

The "preprocessing" stage runs a single job per visit and executes the
following ImSim binaries:
   ancillary/atmosphere_parameters/create_atmosphere
   ancillary/atmopshere/turb2d
   ancillary/atmopshere/cloud
   ancillary/optics_parameters/optics_paramters
   ancillary/tracking/tracking
   ancillary/trim/trim

The "raytrace" stage runs a single job per single-chip exposure (up to
378 jobs per focalplane) and executes the following ImSim binaries:
   raytrace/lsst  (the biggie)
   ancillary/Add_Background/add_background
   ancillary/cosmic_rays/create_rays
   ancillary/e2adc/e2adc

The basic workflow:

1. Set the environment variable IMSIM_SOURCE_PATH to the absolute path
   of the ImSim source directory.  This should contain the compiled
   executables (i.e. you need to have successfully run "./configure"
   and "makeall")
    1a. If the ImSim binaries are in a location that is different from
        the source tree, then you need to define IMSIM_EXEC_PATH as well.

2. generateVisits.py <trimfilelist_filename> <config_filename> <extraid_filename>
   Inputs:
      <trimfilelist_filename>: A file containing the absolute paths to
                               to the metadata_<obshistid>.dat file
			       for each trimfile to be processed, one
			       per line.  NOTE: You must point to the
			       metadata file and not the timefile tarball.
      <config_filename>: The name of the configuration file.  There
			 are two example config files provided in the distro:
			 imsimConfig_workstation.cfg and imsimConfig_minerva.cfg.
			 These follow the Python ConfigParser convention.
			 More documentation on config files is
			 provided below.
      <extraid_filename>: Name of the extraid file.
   What happens:
      generateVisits.py creates 3 tarballs (each containing files
      necessary to run on an execution node) and one script per
      visit.  These are located in "stagePath1" in the config file.
      The scripts contain all the commands to run the preprocessing
      stage for each visit.  The exact contents of these scripts
      depends on the scheduling environment (e.g. shell, PBS, LSF,
      etc).  A manifest of all of the scripts is located in
      stagePath1/preprocessingJobs.lis (including the command required
      to invoke the script...if you just want to blindly run every
      line in the file, just "csh preprocessingJobs.lis").

3. Execute each line in stagePath1/preprocessingJobs.lis.  This will
   run the preprocessing stage.
   What happens:
      The processing script will copy data from "stagePath1" onto the
      execution node, unpack it, execute the preprocessing stage,
      then copy the output to "stagePath2."  At the very end of this
      process, the script will verify that all of the necessary output
      files exist in stagePath2 by running the script verifyFiles.py.
      If file verification is successful:
         1. The script will output "Output file verification completed
            with no errors."
	 2. It will create a list of jobs for the next stage
            (raytracing) that is located in "stagePath2" and named
	    "<obshistid>-f<filter>-Jobs.lis".  You can simply execute
            the commands in this file to run the raytracing jobs.
      If file verification is not successful:
         1. The script will output "Error in verifyFiles.py!"
	 2. The contents of "<obshistid>-f<filter>-Jobs.lis" will
	    instead contain the error output of verifyFiles.py
      For "PBS" mode, the script stdout/stderr of the script will also
      be copies into a log directory "savePath/<obshistid>-f<filter>/log"
      and named "<obshistid>-f<filter>.out".

4. Assuming your preprocessing files were verified successfully,
   execute each line in stagePath2/<obshistid>-f<filter>-Jobs.lis
   What happens:
      The raytracing script will copy data from "stagePath2" onto
      the execution node, unpack it, execute the raytracing stage,
      then copy the output to "savePath".  *Before* copying the
      data to "savePath", the script will run verifyFiles.py to
      verify that all of the expected files were generated, and it
      will also run "fitsverify" on each of the FITS files.
      If file verification is successful:
         1. The script will output "Output files successfully verified."
	 2. It will copy all of the output files to "savePath".
	 3. It will create a file called "<id>.verified" in the
	    directory "savePath/<obshistid>-f<filter>/log" where
	    <id> is of the form "Rxx_Sxx_Exxx".
      If file verification is not successful:
         1. The script will output "Error in verifyFiles.py!  Output
            written to <id>.verify_error" where as above, <id> is of
	    the form "Rxx_Sxx_Exxx".
	 2. It will write the verify errors to "<id>.verify_error" and
	    place this file in the directory
            "savePath/<obshistid>-f<filter>/log".
      For "PBS" mode, the script stdout/stderr of the script will also
      be copies into a log directory "savePath/<obshistid>-f<filter>/log"
      and named "<obshistid>-f<filter>-<id>.out".


==========================
CONFIGURATION FILES
==========================

The format configuration files is largely self-explanatory (just
see the Python ConfigParser documentation for details).  It is made of
sections that are given in square brackets (e.g. "[general]",
"[pbs]").  In each section, one can specify a variable and a value as
follows:
   <variable>: <value>

The most complicated aspect of the configuration files is
understanding the directory structure.  In an effort to be as general
as possible, there are a number of directories.  The good news is that
many of these can be set to the same values (see "simplifications"
below).

There are 3 main storage locations:
   1. On the submission node (i.e. the client node),
   2. On a shared storage volume that is accessible from both the submit
          and the execution nodes.
   3. On the execution nodes.

The directory structure in each location is as follows.  The names of
the variables that must be specified are given in quotes.  The names
of items that are put into these directories is also given for informational
purposes and these are not in quotes.

   submission (visible only from submission node):
       /"IMSIM_SOURCE_PATH"             Absolute path to ImSim source directory tree
       /"IMSIM_EXEC_PATH"               If this is defined, it is the location of the
                                        ImSim executables (for build systems that place
                                        executables in different locations).  If not
                                        defined, executables are assumed to be in IMSIM_HOME_PATH.
                                        Note that IMSIM_EXEC_PATH must still preserve the
                                        same directory structure as IMSIM_HOME_PATH.
   shared storage (visible from both submission and execution nodes):
       /"dataPathPRE"/"dataTarballPRE"  Abs. path to the "preprocessing" tarball containing
                                        QE and height maps (if useSharedPRE is "false")
       /"dataPathPRE"                   Abs. path to the "preprocessing" data directory structure
                                        containing QE and heigh height maps (if useSharedPRE is "true")
       /"dataPathSEDs"/"dataTarballSEDs"Abs. path to the "main" tarball containing SEDs
       /"dataPathSEDs"                  Abs. path to the "main" data directory structure containing
                                        SEDs (if useSharedSEDs is "true")

       /"stagePath1"                    Abs. path to which files are staged before execution
                                        of preprocessing phase
       /"stagePath1"/trimfiles          Staging area for trimfiles
       /"stagePath1"/*_f[filter].[pbs,csh] Preprocessing scripts
       /"stagePath1"/visitFiles*-fr.tar.gz Per-visit files for preprocessing step.
       /"stagePath1"/imsimExecFiles.tar.gz Exec files needed for all preprocessing visits

       /"stagePath2"                    Absolute path for files output by preprocessing
                                        step and staging for raytracing step.
       /"stagePath2"/*-f[filter]        Files for each full focal plane visit
       /"stagePath2"/*-f[filter]/nodefiles*.tar.gz  Files for this visit common to all detectors
       /"stagePath2"/*-f[filter]/run*   Param files for each detector/exposure and atmosphere screens

       /"savePath"                      Abs. path to which output data is written
       /"savePath"/*-f[filter]/logs     log files organized by full focal plane visit
       /"savePath"/imSim                Output images! (Yes, eventually we do generate these!)

   execution (visible only from execution node):
       /"scratchSharedPath"             Abs path to the location of the untarred shared catalog data
       /"scratchExecPath"               Abs. path to execution directory on compute node
       /"scratchExecPath"/<workunitID>  Path to a specific work unit's ImSim directory tree on the exec node
       /"scratchExecPath"/<workunitID>/"scratchOutputDir" Path to output data on the exec node

   <workunitID> is determined at runtime and is of the form:
         <obshistid>-f<filter>-<id> where:
               obshistid:  obshistid including extraID
               filter:     filter letter ID
               id:         full exposure id of the form R[0-4][0-4]_S[0-2][0-2]_E[0-9][0-9][0-9]
         Example: "1111110-fr-R01_S12_E001"

Simplifications:
   - stagePath1, stagePath2, and savePath can all point to the same location if desired.
   - scratchSharedPath and scratchExecPath can also point to same location if desired.
   - dataTarballPRE and dataTarballSEDs can be the same tarball


A note on the shared dataset (QE maps, SEDs, etc):
--------------------------------------------------

The shared dataset can either be staged to the exec node, or a
symbolic link can be made between the execution directory (on the exec
node) and its location in shared storage.  For executing on a
workstation (or anywhere where the shared data location is permanently
on a filesystem local to the execution node) the latter is
recommended.  Selecting this behavior for the preprocessing stage is
achieved by setting "useSharedPRE".  For the raytracing stage, set
"useSharedSEDs".

When executing in a distributed environment, it is recommended that
"useSharedSEDs" be "false".  For some clusters, setting "useSharedPRE"
to "true" might still work well, since the preprocessing stage only
needs to access a single file per focal plane.

!!!!!!!!!!!!!!!!
 IMPORTANT NOTE: Unlike with the "full_focalplane" shell script, the shared data
!!!!!!!!!!!!!!!! tarball used here should *not* contain "data" as the root
                 directory (this just made things too complicated in the script
                 logic).  For example, the directories "focal_plane," agnSED," starSED,"
                 etc should be in the root of the tarball.

==========================
VERIFYING FILES
==========================

"verifyFiles.py" can be used to verify the output of both the
preprocessing and raytrace stages.  The control scripts automatically
verify proprocessing output (in shared storage) as well as raytracing
output (on the execution node).  The details for parsing the output
from each of these steps is given above.

The control scripts do not automatically verify that an entire focalplane
has completed the raytracing stage and been stored in <savePath>.  To
do this, you can execute:
  verifyFiles.py <obshistid> <filter> <savePath>

Note that <savePath> also has to include "imSim/PT1.2" if it is
present (i.e. it should be the path to the "eimage" and "raw"
directories).
