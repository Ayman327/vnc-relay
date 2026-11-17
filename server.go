// relay-server: public relay for the VNC link (runs on a free host, e.g. Render).
// Single Go process — no Durable Object needed.
// Endpoints:
//   /vm          WebSocket: the VM client (vncrelay) keeps this open.
//   /websockify  WebSocket: the phone's noVNC client; bridged to the VM.
//   /*           HTTP: forwarded to the VM's localhost:6080, response returned.
// Protocol over the VM socket is the same as worker.js:
//   Text = JSON control (hello / http / http-resp / ws-open / ws-opened /
//   ws-error / ws-close / ws-closed); Binary = [id:uint16 BE][payload].
package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type ctrlMsg struct {
	T       string            `json:"t"`
	ID      int               `json:"id"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
	Msg     string            `json:"msg"`
}

type pendingHTTP struct {
	done chan ctrlMsg
}

type server struct {
	mu           sync.Mutex
	vmConn       *websocket.Conn
	vmWriteMu    sync.Mutex
	pending      map[int]*pendingHTTP
	phoneSockets map[int]*websocket.Conn
	nextID       int64
}

func (s *server) vmSendText(m ctrlMsg) error {
	b, _ := json.Marshal(m)
	s.vmWriteMu.Lock()
	defer s.vmWriteMu.Unlock()
	if s.vmConn == nil {
		return errNoVM
	}
	return s.vmConn.WriteMessage(websocket.TextMessage, b)
}

func (s *server) vmSendBinary(id int, payload []byte) error {
	frame := make([]byte, 2+len(payload))
	binary.BigEndian.PutUint16(frame, uint16(id))
	copy(frame[2:], payload)
	s.vmWriteMu.Lock()
	defer s.vmWriteMu.Unlock()
	if s.vmConn == nil {
		return errNoVM
	}
	return s.vmConn.WriteMessage(websocket.BinaryMessage, frame)
}

var errNoVM = &simpleErr{"vm not connected"}

type simpleErr struct{ s string }

func (e *simpleErr) Error() string { return e.s }

func (s *server) nextStreamID() int {
	for {
		id := int(atomic.AddInt64(&s.nextID, 1))
		if id == 0 || id > 0xffff {
			atomic.StoreInt64(&s.nextID, 1)
			continue
		}
		return id
	}
}

// --- VM control socket ---
func (s *server) handleVM(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("vm upgrade failed: %v", err)
		return
	}
	s.mu.Lock()
	old := s.vmConn
	s.vmConn = ws
	s.mu.Unlock()
	if old != nil {
		old.Close()
	}
	log.Printf("vm connected from %s", r.RemoteAddr)
	defer func() {
		s.mu.Lock()
		if s.vmConn == ws {
			s.vmConn = nil
		}
		s.mu.Unlock()
		ws.Close()
		// fail pending HTTP + close phone sockets
		s.mu.Lock()
		for id, p := range s.pending {
			p.done <- ctrlMsg{T: "http-resp", ID: id, Status: 502}
			delete(s.pending, id)
		}
		for id, pw := range s.phoneSockets {
			pw.Close()
			delete(s.phoneSockets, id)
		}
		s.mu.Unlock()
		log.Printf("vm disconnected")
	}()
	for {
		mt, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		if mt == websocket.TextMessage {
			var m ctrlMsg
			if err := json.Unmarshal(data, &m); err != nil {
				continue
			}
			if m.T == "http-resp" {
				s.mu.Lock()
				p, ok := s.pending[m.ID]
				if ok {
					delete(s.pending, m.ID)
				}
				s.mu.Unlock()
				if ok {
					select {
					case p.done <- m:
					default:
					}
				}
			} else if m.T == "ws-error" || m.T == "ws-closed" {
				s.mu.Lock()
				pw, ok := s.phoneSockets[m.ID]
				if ok {
					delete(s.phoneSockets, m.ID)
				}
				s.mu.Unlock()
				if ok {
					pw.Close()
				}
			}
			// ws-opened: nothing
		} else if mt == websocket.BinaryMessage {
			if len(data) < 2 {
				continue
			}
			id := int(binary.BigEndian.Uint16(data))
			s.mu.Lock()
			pw, ok := s.phoneSockets[id]
			s.mu.Unlock()
			if ok {
				_ = pw.WriteMessage(websocket.BinaryMessage, data[2:])
			}
		}
	}
}

// --- Phone VNC websocket ---
func (s *server) handleWebsockify(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	vmUp := s.vmConn != nil
	s.mu.Unlock()
	if !vmUp {
		http.Error(w, "vm not connected yet, retry in a few seconds", http.StatusBadGateway)
		return
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("phone ws upgrade failed: %v", err)
		return
	}
	id := s.nextStreamID()
	s.mu.Lock()
	s.phoneSockets[id] = ws
	s.mu.Unlock()
	if err := s.vmSendText(ctrlMsg{T: "ws-open", ID: id, Path: "/websockify"}); err != nil {
		s.mu.Lock()
		delete(s.phoneSockets, id)
		s.mu.Unlock()
		ws.Close()
		return
	}
	defer func() {
		s.mu.Lock()
		delete(s.phoneSockets, id)
		s.mu.Unlock()
		_ = s.vmSendText(ctrlMsg{T: "ws-close", ID: id})
		ws.Close()
	}()
	for {
		mt, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		if mt == websocket.BinaryMessage || mt == websocket.TextMessage {
			_ = s.vmSendBinary(id, data)
		}
	}
}

// --- Blender MCP websocket (another computer's Antigravity) ---
// Same stream protocol as /websockify, but the VM bridges it to the raw
// TCP socket of the Blender addon (127.0.0.1:9876) instead of noVNC.
func (s *server) handleBlender(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	vmUp := s.vmConn != nil
	s.mu.Unlock()
	if !vmUp {
		http.Error(w, "vm not connected yet, retry in a few seconds", http.StatusBadGateway)
		return
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("blender ws upgrade failed: %v", err)
		return
	}
	id := s.nextStreamID()
	s.mu.Lock()
	s.phoneSockets[id] = ws
	s.mu.Unlock()
	if err := s.vmSendText(ctrlMsg{T: "ws-open", ID: id, Path: "/blender"}); err != nil {
		s.mu.Lock()
		delete(s.phoneSockets, id)
		s.mu.Unlock()
		ws.Close()
		return
	}
	defer func() {
		s.mu.Lock()
		delete(s.phoneSockets, id)
		s.mu.Unlock()
		_ = s.vmSendText(ctrlMsg{T: "ws-close", ID: id})
		ws.Close()
	}()
	for {
		mt, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		if mt == websocket.BinaryMessage || mt == websocket.TextMessage {
			_ = s.vmSendBinary(id, data)
		}
	}
}

// --- Phone HTTP (noVNC page + assets) ---
func (s *server) handleHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	vmUp := s.vmConn != nil
	s.mu.Unlock()
	if !vmUp {
		http.Error(w, "vm not connected yet, retry in a few seconds", http.StatusBadGateway)
		return
	}
	id := s.nextStreamID()
	p := &pendingHTTP{done: make(chan ctrlMsg, 1)}
	s.mu.Lock()
	s.pending[id] = p
	s.mu.Unlock()
	// Read the request body (for POST forms) and forward it to the VM.
	var bodyB64 string
	var reqHeaders map[string]string
	if r.Body != nil {
		raw, _ := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if len(raw) > 0 {
			bodyB64 = base64.StdEncoding.EncodeToString(raw)
		}
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		reqHeaders = map[string]string{"Content-Type": ct}
	}
	if err := s.vmSendText(ctrlMsg{T: "http", ID: id, Method: r.Method, Path: r.URL.RequestURI(), Body: bodyB64, Headers: reqHeaders}); err != nil {
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
		http.Error(w, "vm send failed", http.StatusBadGateway)
		return
	}
	select {
	case resp := <-p.done:
		for k, v := range resp.Headers {
			switch {
			case k == "Content-Type" || k == "Content-type":
				w.Header().Set("Content-Type", v)
			case k == "Cache-Control" || k == "Cache-control":
				w.Header().Set("Cache-Control", v)
			}
		}
		status := resp.Status
		if status == 0 {
			status = 200
		}
		w.WriteHeader(status)
		if resp.Body != "" {
			// body is base64 from the VM client
			raw, err := decodeB64(resp.Body)
			if err == nil {
				_, _ = w.Write(raw)
			}
		}
	case <-time.After(15 * time.Second):
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
		http.Error(w, "vm timeout", http.StatusGatewayTimeout)
	}
}

func decodeB64(s string) ([]byte, error) {
	// standard base64 (VM client uses StdEncoding)
	return base64.StdEncoding.DecodeString(s)
}

func main() {
	s := &server{
		pending:      make(map[int]*pendingHTTP),
		phoneSockets: make(map[int]*websocket.Conn),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/vm", s.handleVM)
	mux.HandleFunc("/websockify", s.handleWebsockify)
	mux.HandleFunc("/blender", s.handleBlender)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		vmUp := s.vmConn != nil
		s.mu.Unlock()
		if vmUp {
			w.Write([]byte("ok vm-connected"))
		} else {
			w.Write([]byte("ok vm-waiting"))
		}
	})
	mux.HandleFunc("/", s.handleHTTP)
	port := getenv("PORT", "8080")
	log.Printf("relay-server listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
