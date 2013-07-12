package raft

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
)

var (
	// IDPath is where the ID handler (GET) will be installed in the
	// HTTPTransport.
	IDPath = "/raft/id"

	// AppendEntriesPath is where the AppendEntries RPC handler (POST) will be
	// installed in the HTTPTransport.
	AppendEntriesPath = "/raft/appendentries"

	// RequestVotePath is where the RequestVote RPC handler (POST) will be
	// installed in the HTTPTransport.
	RequestVotePath = "/raft/requestvote"

	// CommandPath is where the Command RPC handler (POST) will be installed in
	// the HTTPTransport.
	CommandPath = "/raft/command"

	// SetConfigurationPath is where the SetConfiguration RPC handler (POST)
	// will be installed in the HTTPTransport.
	SetConfigurationPath = "/raft/setconfiguration"
)

var (
	emptyAppendEntriesResponse bytes.Buffer
	emptyRequestVoteResponse   bytes.Buffer
)

func init() {
	gob.Register(&HTTPPeer{})
	json.NewEncoder(&emptyAppendEntriesResponse).Encode(appendEntriesResponse{})
	json.NewEncoder(&emptyRequestVoteResponse).Encode(RequestVoteResponse{})
}

// HTTPTransport provides a bridge between a Raft Server and the outside world
// through HTTP.
type HTTPTransport struct{}

// Register installs handlers for all of the Raft-domain RPCs in the passed
// ServeMux at predefined paths. All of those handlers will forward their
// received RPCs to the passed Server.
func (t *HTTPTransport) Register(mux *http.ServeMux, s *Server) {
	mux.HandleFunc(IDPath, t.idHandler(s))
	mux.HandleFunc(AppendEntriesPath, t.appendEntriesHandler(s))
	mux.HandleFunc(RequestVotePath, t.requestVoteHandler(s))
	mux.HandleFunc(CommandPath, t.commandHandler(s))
	mux.HandleFunc(SetConfigurationPath, t.setConfigurationHandler(s))
}

func (t *HTTPTransport) idHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(fmt.Sprint(s.ID())))
	}
}

func (t *HTTPTransport) appendEntriesHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var ae appendEntries
		if err := json.NewDecoder(r.Body).Decode(&ae); err != nil {
			http.Error(w, emptyAppendEntriesResponse.String(), http.StatusBadRequest)
			return
		}

		aer := s.appendEntries(ae)
		if err := json.NewEncoder(w).Encode(aer); err != nil {
			http.Error(w, emptyAppendEntriesResponse.String(), http.StatusInternalServerError)
			return
		}
	}
}

func (t *HTTPTransport) requestVoteHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var rv RequestVote
		if err := json.NewDecoder(r.Body).Decode(&rv); err != nil {
			http.Error(w, emptyRequestVoteResponse.String(), http.StatusBadRequest)
			return
		}

		rvr := s.requestVote(rv)
		if err := json.NewEncoder(w).Encode(rvr); err != nil {
			http.Error(w, emptyRequestVoteResponse.String(), http.StatusInternalServerError)
			return
		}
	}
}

func (t *HTTPTransport) commandHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		// TODO we're collapsing a lot of errors into an empty response.
		// If we can decide on an error format, we could propegate them.

		cmd, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		response := make(chan []byte, 1)
		if err := s.Command(cmd, response); err != nil {
			http.Error(w, "", http.StatusInternalServerError)
			return
		}

		resp, ok := <-response
		if !ok {
			http.Error(w, "", http.StatusInternalServerError)
			return
		}

		w.Write(resp)
	}
}

func (t *HTTPTransport) setConfigurationHandler(s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var peers Peers
		if err := gob.NewDecoder(r.Body).Decode(&peers); err != nil {
			errBuf, _ := json.Marshal(commaError{err.Error(), false})
			http.Error(w, string(errBuf), http.StatusBadRequest)
			return
		}

		if err := s.SetConfiguration(peers); err != nil {
			errBuf, _ := json.Marshal(commaError{err.Error(), false})
			http.Error(w, string(errBuf), http.StatusInternalServerError)
			return
		}

		respBuf, _ := json.Marshal(commaError{"", true})
		w.Write(respBuf)
	}
}

// commaError is the structure returned by the configuration handler, to clients
// that make set-configuration requests over the HTTP Transport.
type commaError struct {
	Error   string `json:"error,omitempty"`
	Success bool   `json:"success,omitempty"`
}

// HTTPPeer represents a remote Raft server, which should be exposed to the
// outside world using a HTTP Transport.
type HTTPPeer struct {
	id  uint64
	url url.URL
}

// NewHTTPPeer constructs a new HTTP peer. Part of construction involves
// making a HTTP GET request against the IDPath to resolve the remote server's
// ID.
func NewHTTPPeer(u url.URL) (*HTTPPeer, error) {
	u.Path = ""

	idURL := u
	idURL.Path = IDPath
	resp, err := http.Get(idURL.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	id, err := strconv.ParseUint(string(buf), 10, 64)
	if err != nil {
		return nil, err
	}
	if id <= 0 {
		return nil, fmt.Errorf("invalid peer ID %d", id)
	}

	return &HTTPPeer{
		id:  id,
		url: u,
	}, nil
}

// ID returns the Raft-domain ID retrieved during construction of the HTTPPeer.
func (p *HTTPPeer) ID() uint64 { return p.id }

// AppendEntries triggers a AppendEntries RPC to the remote server, and
// returns the response. Errors at the transport layers are logged, and
// represented by a default (unsuccessful) response
func (p *HTTPPeer) AppendEntries(ae appendEntries) appendEntriesResponse {
	var aer appendEntriesResponse

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(ae); err != nil {
		log.Printf("Raft: HTTP Peer: AppendEntries: encode request: %s", err)
		return aer
	}

	var resp bytes.Buffer
	if err := p.rpc(&body, AppendEntriesPath, &resp); err != nil {
		log.Printf("Raft: HTTP Peer: AppendEntries: during RPC: %s", err)
		return aer
	}

	if err := json.Unmarshal(resp.Bytes(), &aer); err != nil {
		log.Printf("Raft: HTTP Peer: AppendEntries: decode response: %s", err)
		return aer
	}

	return aer
}

// RequestVote triggers a RequestVote RPC to the remote server, and
// returns the response. Errors at the transport layers are logged, and
// represented by a default (unsuccessful) response.
func (p *HTTPPeer) RequestVote(rv RequestVote) RequestVoteResponse {
	var rvr RequestVoteResponse

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(rv); err != nil {
		log.Printf("Raft: HTTP Peer: RequestVote: encode request: %s", err)
		return rvr
	}

	var resp bytes.Buffer
	if err := p.rpc(&body, RequestVotePath, &resp); err != nil {
		log.Printf("Raft: HTTP Peer: RequestVote: during RPC: %s", err)
		return rvr
	}

	if err := json.Unmarshal(resp.Bytes(), &rvr); err != nil {
		log.Printf("Raft: HTTP Peer: RequestVote: decode response: %s", err)
		return rvr
	}

	return rvr
}

// Command forwards the passed cmd to the remote server. Any error at the
// transport or application layer is returned synchronously. If no error
// occurs, the response (the output of the remote server's ApplyFunc) is
// eventually sent on the passed response chan.
func (p *HTTPPeer) Command(cmd []byte, response chan []byte) error {
	errChan := make(chan error)
	go func() {
		var responseBuf bytes.Buffer
		err := p.rpc(bytes.NewBuffer(cmd), CommandPath, &responseBuf)
		errChan <- err
		if err != nil {
			return
		}
		response <- responseBuf.Bytes()
	}()
	return <-errChan // TODO timeout?
}

// SetConfiguration forwards the passed network configuration to the remote
// server. Any error at the transport or application layer is returned
// synchronously. If no error occurs, clients may assume the passed
// configuration has been accepted and will be replicated via joint-consensus.
func (p *HTTPPeer) SetConfiguration(peers Peers) error {
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(&peers); err != nil {
		log.Printf("Raft: HTTP Peer: SetConfiguration: encode request: %s", err)
		return err
	}

	var resp bytes.Buffer
	if err := p.rpc(buf, SetConfigurationPath, &resp); err != nil {
		log.Printf("Raft: HTTP Peer: SetConfiguration: during RPC: %s", err)
		return err
	}

	var commaErr commaError
	if err := json.Unmarshal(resp.Bytes(), &commaErr); err != nil {
		log.Printf("Raft: HTTP Peer: SetConfiguration: decode response: %s", err)
		return err
	}

	if !commaErr.Success {
		return fmt.Errorf(commaErr.Error)
	}
	return nil
}

func (p *HTTPPeer) rpc(request *bytes.Buffer, path string, response *bytes.Buffer) error {
	url := p.url
	url.Path = path
	resp, err := http.Post(url.String(), "application/json", request)
	if err != nil {
		log.Printf("Raft: HTTP Peer: rpc POST: %s", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	n, err := io.Copy(response, resp.Body)
	if err != nil {
		return err
	}
	if l := response.Len(); n < int64(l) {
		return fmt.Errorf("Short read (%d < %d)", n, l)
	}

	return nil
}