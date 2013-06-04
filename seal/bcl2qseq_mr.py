
import subprocess
import sys

def unserialize_cmd_data(data):
    cmd_data = dict()
    for item in data.split(';'):
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
    # If module is not set to '', insert a module load call
    if cmd_dict.get('module'):
        cmd.append("module load %s ;" % cmd_dict['module'])
    # then remove the 'module' key from the dict
    del cmd_dict['module']
    cmd.append(cmd_dict.pop('bclToQseq'))
    # desired output filename...not implemented
    # desired_output_name = cmd_dict['--qseq-file']
    cmd_dict['--qseq-file'] = '/dev/stdout'
    # create cmdline options for all the values in the dict
    for k, v in cmd_dict.iteritems():
        cmd.extend( (k, v) )
    # Turn command array into a string and execute through a shell.  This is so that
    # if we prepended the module load it will be called before the bclToQseq command.
    # TODO: parametrize how the command is launched to enable other cluster set-ups
    full_cmd = [ '/bin/bash', '-l', '-c', ' '.join(cmd) ]
    try:
        p = subprocess.Popen(full_cmd, shell=False, bufsize=16*1024, stdout=subprocess.PIPE)
        for line in p.stdout:
            writer.emit("", line.rstrip("\n"))
        retcode = p.poll()
        if retcode is None:
            raise RuntimeError("Reading stdout from bclToQseq exited before process finished (p.poll() returned None")
        elif retcode != 0:
            raise RuntimeError("Error running bclToQseq! (retcode == %s)" % retcode)
    except Exception:
        print >> sys.stderr, "Exception while trying to run bclToQseq program"
        print >> sys.stderr, "Command:", full_cmd
        raise
