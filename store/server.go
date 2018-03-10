package store

import "io"

type Server struct {
	Addr string
}

func NewServer(addr string) *Server {
	return &Server{
		Addr: addr,
	}
}

func (s *Server) Serve() error {
	return nil
}

func (s *Server) addBlob(ID uint64, rdr io.Reader) error {
	return nil
}
