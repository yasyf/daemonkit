#include "textflag.h"

TEXT ·readUint32(SB), NOSPLIT, $0-12
	MOVQ address+0(FP), AX
	MOVL (AX), AX
	MOVL AX, ret+8(FP)
	RET
