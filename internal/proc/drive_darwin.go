package proc

func (s *Store) drive(c *Child, id identity, session int) {
	clk := clockOrReal(s.clock)
	exited := make(chan status, 1)
	go func() { exited <- awaitExit(c.pid) }()
	s.driveExit(c, id, session, exited, clk)
}
