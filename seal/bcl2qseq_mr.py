
from copy import copy
import os
import subprocess
import sys
import time

def unserialize_cmd_data(data):
    cmd_data = dict()
    for item in data.split(';'):
        if not item:
            continue # the last semi-colon generates an empty string. We'll get rid of it here.
        tpl = item.split(':', 1)
        if len(tpl) == 1:
            cmd_data[tpl[0]] = ''
        else:
            cmd_data[tpl[0]] = tpl[1]
    return cmd_data


def mapper(k, cmd_data, writer):
    cmd_dict = unserialize_cmd_data(cmd_data)
    # form command
    cmd = []
    program_env = copy(os.environ)
    extra_ld_library_path = cmd_dict.pop('ld_library_path')
    if extra_ld_library_path:
        program_env['LD_LIBRARY_PATH'] = extra_ld_library_path + os.pathsep + program_env.get('LD_LIBRARY_PATH', '')
    cmd.append(cmd_dict.pop('bclToQseq'))
    # desired output filename...not implemented
    # desired_output_name = cmd_dict['--qseq-file']
    cmd_dict['--qseq-file'] = '/dev/stdout'
    # create cmdline options for all the values in the dict
    for k, v in cmd_dict.iteritems():
        cmd.append(k)
        if v:
            cmd.append(v)
    try:
        p = subprocess.Popen(cmd, shell=False, bufsize=16*1024, stdout=subprocess.PIPE, env=program_env)
        for line in p.stdout:
            writer.emit("", line.rstrip("\n"))
        # bclToQseq closed its output stream so we exited the loop above.
        # Sometimes I see that the program takes a moment to exit after
        # closing the stream.  To avoid failing unnecessarily we wait
        # a bit polling the process (p.poll)
        deadline = time.time() + 2 # wait for two seconds
        retcode = p.poll()
        while retcode is None and time.time() < deadline:
            retcode = p.poll()
            time.sleep(0.2)
        if retcode is None:
            raise RuntimeError("bclToQseq closed its output stream but it's not exiting (p.poll() returned None)")
        elif retcode != 0:
            raise RuntimeError("Error running bclToQseq! (retcode == %s)" % retcode)
    except Exception:
        print >> sys.stderr, "Exception while trying to run bclToQseq program"
        print >> sys.stderr, "Command:", cmd
        raise
