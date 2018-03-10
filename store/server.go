package store

import (
	"io"
	"net/http"
)

type Server struct {
	Addr string
}

func NewServer(addr string) *Server {
	return &Server{
		Addr: addr,
	}
}

func (s *Server) handler(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, "hello, world!\n")
}

func (s *Server) Serve() error {
	http.HandleFunc("/", s.handler)
	return http.ListenAndServe(s.Addr, nil)
}

func (s *Server) addBlob(ID uint64, rdr io.Reader) error {
	return nil
}
