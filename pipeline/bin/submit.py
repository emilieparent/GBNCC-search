#!/usr/bin/env python
import os, subprocess, shutil, glob, time, datetime, pytz, config, utils, database, pyslurm, time
prestodir = os.environ["PRESTO"]

#checkpoints = glob.glob(os.path.join(config.jobsdir, "*.checkpoint"))
checkpoints = []

queue = pyslurm.job() 

print("Starting GBNCC job submitter...")

while True:
    print("Connecting to database")
    db = database.Database("observations")
    query = "SELECT ProcessingID,FileName FROM GBNCC WHERE "\
                        "ProcessingStatus='i'"
    print("Updating job states")
    db.execute(query)
    ret = db.fetchall()
    if len(ret) != 0:
        print("Getting all currently running jobs")
        alljobs = queue.get()
        if alljobs is not None:
            for j in alljobs:
                for jobid,filenm in ret:
                    if filenm.split('.')[0] in alljobs[j]['name'] and alljobs[j]['job_state']=='RUNNING':
                        nodenm = alljobs[j]["alloc_node"]
                        jobnm  = alljobs[j]["name"]
                        #checkpoint = os.path.join(config.jobsdir, jobnm+".checkpoint")
                        #with open(checkpoint, "w") as f:
                        #    f.write(nodenm+"\n")
                        #    f.write("0 0\n")
                        query = "UPDATE GBNCC SET ProcessingStatus='p' "\
                            "WHERE FileName='{filenm}'".format(filenm=filenm)
                        db.execute(query)
                    else:
                        pass
        db.close()
    filenms = glob.glob(os.path.join(config.datadir, "guppi*GBNCC*fits"))
    print("Getting number of jobs in queue")
    nqueued = utils.getqueue(config.machine,queue)
    print("\t %s jobs in queue \n"%str(nqueued))

    while nqueued<config.queuelim and (len(filenms)>0 or len(checkpoints)>0):
        if len(checkpoints) > 0:
            checkpoint = checkpoints.pop()
            basenm,hashnm = checkpoint.split(".")[0:2]
            basenm = os.path.basename(basenm)
            filenm = basenm + ".fits"
            #with open(checkpoint, "r") as f:
            #    nodenm = f.readline().strip()
            #    jobid  = f.readline().strip()
        
        elif len(filenms) > 0:
            filenm = filenms.pop()
            shutil.move(filenm, os.path.join(config.datadir, "holding/"))
            filenm = os.path.join(config.datadir, "holding",
                                  os.path.basename(filenm))
            basenm = os.path.basename(filenm).rstrip(".fits")
            hashnm = os.urandom(8).encode("hex")
            nodenm = "1"
            jobid  = "$SLURM_JOBID"
        
        jobnm   = basenm + "." +  hashnm
	
        workdir = os.path.join(config.baseworkdir, jobid, basenm, hashnm)
        tmpdir  = os.path.join(config.basetmpdir, jobid, basenm, hashnm, "tmp")

	mjd = str(basenm.split('guppi_')[-1]).split('_GBNCC')[0]
	srcnm = 'GBNCC'+str(basenm.split('_GBNCC')[-1]).split('_')[0]
	resultsdir = os.path.join(config.baseoutdir, mjd, srcnm)

        subfilenm = os.path.join(config.jobsdir, jobnm+".sh")
        subfilenm_rt = subfilenm.split('/')[-1]
        subfile   = open(subfilenm, "w")
        subfile.write(config.subscript.format(filenm=filenm, basenm=basenm, 
                                              jobnm=jobnm, workdir=workdir,
                                              baseworkdir=config.baseworkdir,
                                              hashnm=hashnm, jobid=jobid,
                                              jobsdir=config.jobsdir,
                                              tmpdir=tmpdir, 
                                              outdir=config.baseoutdir,
                                              resultsdir=resultsdir,					
                                              logsdir=config.logsdir,
                                              nodenm=nodenm, 
                                              zaplist=config.zaplist,
                                              pipelinedir=config.pipelinedir,
                                              walltimelim=config.walltimelim, 
                                              email=config.email))
        subfile.close()
        jobid,msg = utils.subjob(config.machine,subfilenm,options=" -o {0}/{1}.log -e {0}/{1}.err".format(config.logsdir,subfilenm_rt))
        #jobid,msg = utils.subjob(config.machine,subfilenm,options=" -o {0}/{1}.log -e {0}/{1}.log".format(config.logsdir,subfilenm_rt))
        #jobid,msg = utils.subjob(config.machine,subfilenm,options=" -o {0} -e {0}".format(config.logsdir))
        if jobid is None: 
            print("ERROR: %s: %s"%(jobnm,msg))
        
        else:
            jobid=int(jobid.split()[-1])
            print("Submitted %s with ID %s"%(jobnm,jobid))
            prestoversion = subprocess.Popen("cd %s ; git rev-parse HEAD 2> /dev/null"%prestodir,shell=True,stdout=subprocess.PIPE).stdout.readline().strip()
            #jobid = jobid.strip()
            time.sleep(10)
            ntries = 0
            success = False
            status = "f"
            alljobs = queue.get()
#            while not success and ntries <= 2:
            while not success and ntries <= 25:
                if alljobs is not None:
                    #exit()
                    try:
                        alljobs[jobid]
                        if alljobs[jobid]["job_state"] == "PENDING":
                            success = True
                            status = "i"
                        else:
                            success = True
                            status = "p"
                            nodenm = alljobs[jobid]["exec_host"][0]
                            checkpoint = os.path.join(config.jobsdir, jobnm+".checkpoint")
                            with open(checkpoint, "w") as f:
                                f.write(nodenm+"\n")
                                f.write("0 0\n")
                    except:
                        print("Waiting to find job")
                        time.sleep(5)
                        ntries += 1
                        alljobs = queue.get()
            print("Marked job with status '%s'"%status)
            date = datetime.datetime.now()
            query = "UPDATE GBNCC SET ProcessingStatus='{status}',"\
                "ProcessingID='{jobid}',ProcessingSite='{site}',"\
                "ProcessingAttempts=ProcessingAttempts+1,"\
                "ProcessingDate='{date}',"\
                "PipelineVersion='{version}', "\
                "PRESTOVersion='{prestoversion}' "\
                "WHERE FileName='{filenm}'".format(status=status,
                                                   jobid=jobid,
                                                   site=config.machine,
                                                   date=date.isoformat(),
                                                   version=config.version,
                                                   prestoversion=prestoversion,
                                                   filenm=os.path.basename(filenm))
                
            db = database.Database("observations")
            db.execute(query)
            db.commit()
            db.close()
        nqueued = utils.getqueue(config.machine,queue)
            
    else:
        if nqueued>=config.queuelim:
            print("Queue full.  Sleeping...\n")
        elif len(filenms) == 0:
            print("Nothing to submit.  Sleeping...\n")
        time.sleep(config.sleeptime)
