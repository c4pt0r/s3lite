package store

import (
	"io"
	"net/http"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	Addr string

	store *Store
}

func NewServer(addr string) *Server {
	return &Server{
		Addr: addr,
	}
}

func (s *Server) OpenStore(storePath string) error {
	store := new(Store)
	if err := store.Open(storePath, false); err != nil {
		return errors.Trace(err)
	}
	s.store = store
	return nil
}

func (s *Server) handler(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, "hello, world!\n")
}

func (s *Server) Serve() error {
	log.Info("HTTP server starts serving...", s.Addr)
	http.HandleFunc("/", s.handler)
	return http.ListenAndServe(s.Addr, nil)
}
