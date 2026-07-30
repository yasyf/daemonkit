package daemonkit

import (
	"context"

	"github.com/yasyf/daemonkit/internal/proc"
)

// Run executes one bounded disposable command under durable process ownership,
// reap included. ctx must carry a deadline: the run's whole budget — spawn,
// streams, termination, settlement — derives from it and nothing else, with the
// terminate ladder reserved a tail so settlement is never starved.
//
// store is the interim spawn authority. proc.Cmd and proc.Result are internal
// types, so no external module can call Run yet; at P3 it unexports and Ctx.Run
// becomes its one caller, closing over the owner.
func Run(ctx context.Context, store *proc.Store, c proc.Cmd) (proc.Result, error) {
	return store.Run(ctx, c)
}
