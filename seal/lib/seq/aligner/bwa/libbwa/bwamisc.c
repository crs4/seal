
#include <stdio.h>

#ifdef BWT_EXPORT_LIBRARY_FUNCTIONS

/* We need the macro PACKAGE_VERSION from main.c to be defined in here, but we 
 * don't want to duplicated it.  A working solution is to include main.c.
 * In main.c we've wrapped all significant content within 
 * ifndef BWT_EXPORT_LIBRARY_FUNCTIONS
 * so that it's mutual exclusive with this file's content.
 */
#include "main.c"

void bwa_print_sam_PG()
{
	printf("@PG\tID:bwa\tPN:bwa\tVN:%s\n", PACKAGE_VERSION);
}
#endif
