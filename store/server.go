package store

import (
	"io"
	"net/http"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	store *Store
}

func (s *Server) Init(storeID string, initPeers []string) error {
	store := NewStoreWithIDAndConfig(storeID, NewDefaultStoreConfig())
	if err := store.Open(false); err != nil {
		return errors.Trace(err)
	}
	store.Join("", "", 0, initPeers)
	s.store = store
	return nil
}

func (s *Server) handler(w http.ResponseWriter, req *http.Request) {
	io.WriteString(w, "hello, world!\n")
}

func (s *Server) Serve(addr string) error {
	log.Info("HTTP server starts serving...", addr)
	http.HandleFunc("/", s.handler)
	return http.ListenAndServe(addr, nil)
}
