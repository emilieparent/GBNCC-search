import os, subprocess

# Name of institution where pipeline is being run
institution = "McGill"
# Name of HPC machine where pipeline is being run
machine     = "beluga"
# Timezone of processing site
timezone    = "Canada/Eastern"
# User name on 'machine'
#user        = "rlynch"
user        = "aemcwen"
# pyslurm user identification number 
user_id     = 3085590                                                               
# Email address where job notifications will be sent (if enabled)
email       = "rlynch+gbncc_jobs@physics.mcgill.ca"
# Walltime limit (hh:mm:ss)
walltimelim = "96:00:00"
# Maximum size of the 'pending' job queue
queuelim    = 1000
# Time to wait between submitting a new job when there are no new files or the
# 'pending' queue is full
sleeptime   = 5*60
# Disk quota size of datadir (in bytes)
datadir_lim = 14*1024**4 - 12*1024**3 # 10 TB - 12 GB of overhead
# Top level analysis directory
topdir      = "/project/rrg-vkaspi-ad/GBNCC"                   
topdirtmp   = "/scratch/aemcewen"
# Base working directory for data reduction (should have at least 13 GB free)
baseworkdir = "$SLURM_TMPDIR"
# Base temporary directory for data reduction (should have at least 2 GB free) 
basetmpdir  =  "$SLURM_TMPDIR"                                         
# Directory where pipeline scripts are stored
pipelinedir = os.path.join(topdir, "pipeline")
# Directory where raw data files are stored before being processed
datadir     = "/scratch/aemcewen/GBNCC"

# Destination host to for uploading results
desthost = "lore4.physics.mcgill.ca"
# Destination directory on desthost for uploading results
destdir = "/data/lore4/GBNCC/results"

# Directory where job submission files are stored
jobsdir     = os.path.join(topdirtmp, "jobs")
# Directory where log files are stored
logsdir     = os.path.join(topdirtmp, "logs")
# Directory where output files are permanently stored
baseoutdir  = os.path.join(topdirtmp, "results")
baseoutdir_final  = os.path.join(topdir, "results")
# Location of FFT zaplist
zaplist     = os.path.join(pipelinedir, "lib", "GBNCC.zaplist")
# Pipeline version (as the git hash)
version     = subprocess.Popen("cd %s ; git rev-parse HEAD 2> /dev/null"%pipelinedir,shell=True,stdout=subprocess.PIPE).stdout.readline().strip()
# Databases dictionary
DATABASES = {
    "observations" : {
    "dbnm"   : "GBTDataDB",
    "hostnm" : "pulsar.physics.mcgill.ca",
    "usernm" : "gbtpsr",
    "passwd" : "NRAO100-m",
        },
    }


# Dictionary for holding job submission scripts

subscripts = {"beluga":

"""#!/bin/bash
#SBATCH --job-name={jobnm}
#SBATCH --mail-user={email}
#SBATCH --qos=sw
#SBATCH --nodes={nodenm}
#SBATCH --ntasks-per-node=1
#SBATCH --mem=10000M
#SBATCH --time={walltimelim}
#SBATCH --account=rrg-vkaspi-ad

if [ {nodenm} == 1 ]
  then
    echo -e \"$HOSTNAME
{jobid}
0 0\" > {jobsdir}/{jobnm}.checkpoint
    mkdir -p {workdir}
    mv {filenm} {workdir}
    cp {zaplist} {workdir}
  else
    set -- $({jobsdir}/{jobnm}.checkpoint)
    echo -e \"$HOSTNAME
{jobid}
$3 $4\" > {jobsdir}/{jobnm}.checkpoint
    mv {baseworkdir}/$2 {baseworkdir}/{jobid}
fi
cd {workdir}
mkdir tmp
echo {workdir}
python /project/rrg-vkaspi-ad/GBNCC/pipeline/bin/search.py -w {workdir} -i {hashnm} {basenm}.fits 
"""
}



subscript = subscripts[machine]
