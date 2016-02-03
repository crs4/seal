

flags_to_eliminate = ~(0x400 | 0x20 | 0x8)

# clear all flags pertaining to mate
# clear duplicate read flag
# eliminate all tags except for MD and XC

def mapper(_, line, writer):
	if not line.startswith('@'):
		fields = line.split("\t")
		fields[1] = str(int(fields[1]) & flags_to_eliminate)
		new_fields = [ s for idx, s in enumerate(fields) if idx <= 10 or s.startswith("MD:Z") or s.startswith("XC:i:") ]
		writer.emit("", "\t".join(new_fields))
