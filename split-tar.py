import tarfile
import os
import humanize
from tqdm import tqdm
import io


def get_chunk_size(t, num):

	total_file_size = 0
	for f in t.getmembers():
		total_file_size += f.size

	retval = (total_file_size, int(total_file_size / num))

	return(retval)


def open_chunkfile(file, part, num):

	out = None
	num_len = len(str(num))
	part_formatted = str(part).zfill(num_len)

	filename = os.path.join(folder, f"{os.path.basename(file)}-part-{part_formatted}-of-{num}")
	if not dry_run:
		out = tarfile.open(filename, "w")

	return(filename, out)

def make_merge_scripts(original_tar_file, num_parts):

    basename = os.path.basename(original_tar_file)
    print(basename)
    name = os.path.splitext(basename)[0]

    with open('merge_parts.bat', 'w') as f:
            f.write('@echo off\n')
            f.write('setlocal EnableDelayedExpansion\n')
            f.write('set "startTime=%time: =0%"\n')
            f.write('ECHO Creating folder: '+name+'\n')
            f.write('mkdir "'+name+'"\n')
            f.write('FOR /L %%A IN (1,1,'+str(num_parts)+') DO (\n')
            f.write('   mkdir part_%%A\n')
            f.write('   tar -xvf '+basename+'-part-%%A-of-'+str(num_parts)+' --strip-components 1 -C part_%%A\n')
            f.write('   move part_%%A'+chr(92)+'* '+name+chr(92)+'\n')
            f.write('   rd /q part_%%A\n')
            f.write(')\n')
            
            f.write('set "endTime=%time: =0%"\n')
            f.write('set "end=!endTime:%time:~8,1%=%%100)*100+1!"\n')
            f.write('set "start=!startTime:%time:~8,1%=%%100)*100+1!"\n')
            f.write('set /A "elap=((((10!end:%time:~2,1%=%%100)*60+1!%%100)-((((10!start:%time:~2,1%=%%100)*60+1!%%100), elap-=(elap>>31)*24*60*60*100"\n')
            f.write('set /A "cc=elap%%100+100,elap/=100,ss=elap%%60+100,elap/=60,mm=elap%%60+100,hh=elap/60+100"\n')
            f.write('echo End:      %endTime%\n')
            f.write('echo Elapsed:  %hh:~1%%time:~2,1%%mm:~1%%time:~2,1%%ss:~1%%time:~8,1%%cc:~1%\n')


    with io.open('merge_parts.bash', 'w', newline='\n') as f:
            f.write('#!/bin/bash\n')
            f.write('SECONDS=0\n')
            f.write('mkdir '+name+'\n')
            f.write('for i in {1..'+str(num_parts)+'}\n')
            f.write('do\n')
            f.write('   mkdir part_$i\n')
            f.write('   tar -xvf '+basename+'-part-$i-of-'+str(num_parts)+' --strip-components 1 -C part_$i\n')
            f.write('   mv part_$i'+chr(47)+'* '+name+chr(47)+'\n')
            f.write('   rm -rf part_$i\n')
            f.write('done\n')
            f.write('duration=$SECONDS\n')
            f.write('echo "Process complete. $((duration / 60)) minutes and $((duration % 60)) seconds elapsed."\n')




if __name__ == "__main__":

    original_tar_file = os.path.join('F:\\','ARCHIVES','DEG','DEG-','11314120539-DEG-HEMI-CORR_SHOT_GATHER_SEGY.tar')
    num_parts = 8


    path_file = os.path.split(original_tar_file)
    folder = path_file[0]
	
    t = tarfile.open(original_tar_file, "r")
	
    print(f"Welcome to Tarsplit! Reading file {original_tar_file}...")
	
    (total_file_size, chunk_size) = get_chunk_size(t, num_parts)
	
    print(f"Total uncompressed file size: {humanize.naturalsize(total_file_size, binary = True)} bytes, "
		+ f"num chunks: {num_parts}, chunk size: {humanize.naturalsize(chunk_size, binary = True)} bytes")
	
    (filename, out) = open_chunkfile(original_tar_file, 1, num_parts)
	
    size = 0
    current_chunk = 1
    num_files_in_current_chunk = 0

    num_files = len(t.getmembers())
    pbar = tqdm(total = num_files)

	
	# Loop through our files, and write them out to separate tarballs.
    for f in t.getmembers():
        name = f.name
        size += f.size

        if name[len(name) - 1] == "/":
            print(f"File {name} ends in Slash, skipping due to a bug in the tarfile module. (Directory will still be created by files within that directory)")
            continue

        f = t.extractfile(name)
        info = t.getmember(name)
		

        num_files_in_current_chunk += 1
        pbar.update(1)

        if current_chunk < num_parts:
            if size >= chunk_size:

                print(f"Would have written {humanize.naturalsize(size, binary = True)}"
						+ f" bytes in {num_files_in_current_chunk} files to {filename}")

                size = 0
                current_chunk += 1
                num_files_in_current_chunk = 0

                (filename, out) = open_chunkfile(original_tar_file, current_chunk, num_parts)

            pbar.set_description(f"Writing split tarfile: {current_chunk} of {num_parts}")

    t.close()
    pbar.close()

    print('Creating 2 scripts to reassemble the parts')

    make_merge_scripts(original_tar_file, num_parts)



        




