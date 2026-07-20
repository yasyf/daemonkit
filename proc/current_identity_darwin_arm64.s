#include "textflag.h"

TEXT ·readUint32(SB), NOSPLIT, $0-12
	MOVD address+0(FP), R0
	MOVWU (R0), R0
	MOVW R0, ret+8(FP)
	RET
