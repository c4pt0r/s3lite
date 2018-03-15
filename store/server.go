package store

import (
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
)

type Server struct {
	store *Store
}

func (s *Server) Init(storeID string, createIfExists bool, initPeers []string) error {
	if len(storeID) == 0 {
		storeID = "store-" + uuid.NewV1().String()
	}
	store := NewStoreWithIDAndConfig(storeID, NewDefaultStoreConfig())
	if err := store.Open(createIfExists); err != nil {
		return errors.Trace(err)
	}
	store.Join("", "", 0, initPeers)
	s.store = store
	return nil
}

func (s *Server) doGet(w http.ResponseWriter, r *http.Request, id uint64) {
	vars := mux.Vars(r)
	idStr := vars["id"]

	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	n, err := s.store.ReadID(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if n == nil {
		w.WriteHeader(404)
		return
	}

	w.Write(n.Data)
}

func (s *Server) doDelete(w http.ResponseWriter, r *http.Request, id uint64) {
}

func (s *Server) doPut(w http.ResponseWriter, r *http.Request, id uint64) {
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	n := NewNeedle(id, buf)
	offset, sz, err := s.store.WriteNeedle(n, false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Info("add new needle: ", id, offset, sz)
	w.Write([]byte("OK"))
}

func (s *Server) handler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	idStr := vars["id"]

	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	switch r.Method {
	case "GET":
		s.doGet(w, r, id)
	case "DELETE":
		s.doDelete(w, r, id)
	case "POST":
		fallthrough
	case "PUT":
		s.doPut(w, r, id)
	default:
		http.Error(w, "invalid request method", http.StatusInternalServerError)
		return
	}
}

func (s *Server) Serve(addr string) error {
	log.Info("HTTP server starts serving...", addr)
	r := mux.NewRouter()
	r.HandleFunc("/id/{id}", s.handler)
	return http.ListenAndServe(addr, r)
}
