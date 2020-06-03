#!/usr/bin/env python
import os, subprocess, shutil, glob, time, datetime, pytz, config, utils, database, pyslurm, time
import database as DB
import tarfile
import getpass
from datetime import date 



def move_results():
	to_move = glob.glob(config.baseoutdir+'/*/*/tobeviewed')
	print "Will try to move results of %s jobs"%str(len(to_move))
	k=0
	for t in to_move:
		dirnm = t.split('/tobeviewed')[0]
		dir_subnm = dirnm.split(config.baseoutdir+'/')[-1]
		newdir = os.path.join(config.baseoutdir_final,dir_subnm)
		resultsglob = ['/*.report','/*.tgz','/tobeviewed']
		files = []
		for result in resultsglob:
			filenm = glob.glob(dirnm+result)
			if len(filenm)>0:
				files.append(filenm[0])
			else:
				print "\t\t Missing file %s, continuing .."%dirnm+result 
		try:
                        try:
                                os.makedirs(newdir)
                        except:
                                start, end = newdir.split('results/')
                                end = end.replace('/','b/')
                                newdir = "%sresults/%s"%(start,end)
                                os.makedirs(newdir)
			#os.chmod(newdir,0775)
			#Ensure all files are accessible by both aemcewen & rlynch
    			cmd = "setfacl -R -m u:aemcewen:rwx -m u:rlynch:rxw %s"%newdir
    			subprocess.call(cmd,shell=True)
		except:
			print " \t Directory in project area exists for %s. Will move results if empty "%newdir
	
		if len(glob.glob(newdir+'/*'))==0:
			#New directory is empty, as it should be 
			print "\t Moving content of %s to %s"%(dirnm,newdir)
			for f in files:
				cmd = "rsync %s %s/"%(f,newdir)
				subprocess.call(cmd,shell=True)
				if '.fits' not in f:
					os.chmod(os.path.join(newdir,f),0664)
					os.remove(f)
					
			k+=1
		else: 
			#There are already some results in new directory.. Check if those are older
			oldfile = glob.glob(newdir+'/*')
			old_time = time.ctime(os.path.getctime(oldfile[0]))
			new_time = time.ctime(os.path.getctime(files[0]))
                        """
			if new_time>old_time:
				#if results already there are older, replace by newer results
				print "\t \t Moving content to %s"%(newdir)
				for f in files:
					cmd = "rsync %s %s/"%(f,newdir)
					subprocess.call(cmd,shell=True)
					os.chmod(os.path.join(newdir,f),0664)
					os.remove(f)
				
			else:
				print "\t \t Content in project area newer, skipping..."
                        """
                        print "\t \t Moving content to %s"%(newdir)
                        for f in files:
                            cmd = "rsync %s %s/"%(f,newdir)
                            subprocess.call(cmd,shell=True)
                            os.chmod(os.path.join(newdir,f),0664)
                            os.remove(f)

	print "Moved %d job results"%k 

def results_status(content, basenm):
    npfdplots, nspplots, nratings, nrfifinds, ntgzs, ndiagnostic, nreport = 0,0,0,0,0,0,0
    naccels, ngroup, ngroupplots, nffasummary, nspdplots, nspdratings = 0,0,0,0,0,0

    for c in content:
	if c.startswith(basenm):
		if c.endswith(".pfd.png"):
			npfdplots+=1
		if c.endswith("singlepulse.png"):
			nspplots+=1
		if c.endswith(".pfd.ratings"):
			nratings+=1
		if "rfifind" in c:
			nrfifinds+=1
		if c.endswith(".tgz"):
			ntgzs+=1
		if c.endswith(".report"):
			nreport+=1
		if c.endswith(".diagnostics"):
			ndiagnostic+=1
		if "accelcands" in c:
			naccels+=1
	if c=='groups.txt':
		ngroup+=1
	if c.startswith("grouped") and c.endswith("png"):
		ngroupplots+=1
	if c.endswith("ffacands.summary"):
		nffasummary+=1
	if c.endswith("spd.png"):
		nspdplots+=1
	if c.endswith("spd.rat"):
		nspdratings+=1

    if (naccels != 2) or (ntgzs >10) or (ntgzs < 9) or (nrfifinds < 7) or (nreport != 2) or \
       (npfdplots == 0) or (nspplots < 6) or (nffasummary != 1):
        return "f"
    elif (npfdplots != nratings) or (ndiagnostic != 1) or (ngroup != 1) or \
         (ngroupplots == 0) or (nspdplots != nspdratings):
        return "w"
    else:
        return "s"


move_results()

print("Starting GBNCC job tracker...")
queue = pyslurm.job()#PBSQuery.PBSQuery()

db    = DB.Database("observations")
query = "SELECT ID,ProcessingID,FileName FROM GBNCC WHERE (ProcessingStatus='p' OR ProcessingStatus='i') AND ProcessingSite='%s'"%config.machine
db.execute(query)
ret   = db.fetchall()
alljobs = queue.find_user('aemcewen')

jobs_to_upload = []
today = str(date.today()).replace('-','') 
filenm = config.baseoutdir_final+"/to_rsync_%s.txt"%today
to_upload_file = open(filenm,'w')
if len(ret) != 0 and alljobs is not None:
        for ID,jobid,filenm in ret:
            try: 
                alljobs[int(jobid)]
		print("%s is on the job scheduler, skipping.."%jobid)
            except: 
                MJD,beamid = filenm.split("_")[1:3]
                basenm = filenm.strip(".fits")
                outdir = os.path.join(config.baseoutdir_final, MJD, beamid)
                if not os.path.exists(outdir):
                    MJD = MJD+'b'
                    outdir = os.path.join(config.baseoutdir_final, MJD, beamid)


		try:
			tarball = glob.glob(outdir+'/'+basenm+'*.tgz')[0]
			tar = tarfile.open(tarball,'r:gz')
			content = tar.getnames()
			tar.close()

                	status = results_status(content,basenm)
		except:
			status = "f"
                print("Job %s (guppi_%s_%s) completed with status %s"%(jobid,MJD,basenm,status))
                if status == "s" or status == "w":
                    jobs_to_upload.append(outdir.replace("results","results/."))
		    to_upload_file.write(outdir.replace("results","results/.")+'\n')
                query = "UPDATE GBNCC SET ProcessingStatus='%s' WHERE ID=%i"%(status,ID)
                db.execute(query)
db.close()
to_upload_file.close()
if jobs_to_upload:
    usernm =  getpass.getuser()
    if usernm=='aemcewen':
	usernm = 'mcewena'
    #cmd = "rsync -uvxPltD -p --chmod=Du=rw,Dg=rw,Do=rw,Fu=rwx,Fg=rwx,Fo=rwx --relative {upload_list} {user}@{desthost}:{destdir}".format(upload_list=" ".join(jobs_to_upload), user=usernm, desthost=config.desthost, destdir=config.destdir)
    cmd = "rsync -auvxP --relative {upload_list} {user}@{desthost}:{destdir}".format(
        upload_list=" ".join(jobs_to_upload), user=usernm, desthost=config.desthost, destdir=config.destdir)
    #print(cmd)
    os.system(cmd)
