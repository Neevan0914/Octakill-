package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Matchmaking and relay for online matches.
//
// A player queues for a game mode. Players queueing for the same mode share
// a lobby that waits up to lobbyWait for more people. When the wait ends the
// lobby starts if it has 2+ players (the game fills the remaining slots with
// bots); a lone player is told to play solo. Once started, the server only
// relays messages: the lobby's host client runs the bots and the match rules,
// and if the host leaves (or yields) the next player takes over.

const (
	lobbyWait   = 10 * time.Second
	maxMsgBytes = 8192
	sendBuffer  = 512
)

// Players per match for each mode, matching the game's lobby sizes.
var modeCaps = map[string]int{
	"ffa": 8, "team": 8, "koth": 8, "koth_team": 8, "gungame": 8,
	"deathmatch": 8, "deathmatch_team": 8, "duel": 2,
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// The game page may be hosted elsewhere (GitHub Pages, itch.io, ...).
	CheckOrigin: func(r *http.Request) bool { return true },
}

type player struct {
	id    string
	name  string
	send  chan []byte
	lobby *lobby
}

type lobby struct {
	mode     string
	cap      int
	started  bool
	deadline time.Time
	players  []*player
	host     *player
}

type hub struct {
	mu      sync.Mutex
	nextID  int
	waiting map[string]*lobby // mode -> lobby still collecting players
}

func newHub() *hub {
	h := &hub{waiting: map[string]*lobby{}}
	go h.lobbyTimer()
	return h
}

func encode(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		log.Printf("encode: %v", err)
	}
	return b
}

func (p *player) deliver(msg []byte) {
	select {
	case p.send <- msg:
	default: // client too slow; drop rather than stall the lobby
	}
}

// The methods below must be called with h.mu held.

func (l *lobby) broadcast(msg []byte, except *player) {
	for _, p := range l.players {
		if p != except {
			p.deliver(msg)
		}
	}
}

func (l *lobby) find(id string) *player {
	for _, p := range l.players {
		if p.id == id {
			return p
		}
	}
	return nil
}

func (l *lobby) sendStatus() {
	wait := int(time.Until(l.deadline).Seconds() + 0.999)
	l.broadcast(encode(map[string]any{"t": "lobby", "n": len(l.players), "cap": l.cap, "wait": wait}), nil)
}

func (h *hub) start(l *lobby) {
	delete(h.waiting, l.mode)
	l.started = true
	l.host = l.players[0]
	roster := make([]map[string]string, 0, len(l.players))
	for _, p := range l.players {
		roster = append(roster, map[string]string{"id": p.id, "name": p.name})
	}
	for _, p := range l.players {
		p.deliver(encode(map[string]any{"t": "start", "you": p.id, "host": l.host.id, "mode": l.mode, "players": roster}))
	}
	log.Printf("match started: %s with %d players", l.mode, len(l.players))
}

func (h *hub) lobbyTimer() {
	for range time.Tick(250 * time.Millisecond) {
		h.mu.Lock()
		for mode, l := range h.waiting {
			if time.Now().Before(l.deadline) {
				continue
			}
			if len(l.players) >= 2 {
				h.start(l)
			} else {
				delete(h.waiting, mode)
				for _, p := range l.players {
					p.lobby = nil
					p.deliver(encode(map[string]any{"t": "solo"}))
				}
			}
		}
		h.mu.Unlock()
	}
}

func cleanName(s string) string {
	s = strings.TrimSpace(strings.Map(func(r rune) rune {
		if r < 32 || r == '<' || r == '>' || r == '&' || r == '"' || r == '\'' {
			return -1
		}
		return r
	}, s))
	if runes := []rune(s); len(runes) > 16 {
		s = string(runes[:16])
	}
	if s == "" {
		s = "Player"
	}
	return s
}

func (h *hub) queue(name, mode string) (*player, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	capacity, ok := modeCaps[mode]
	if !ok {
		return nil, false
	}
	h.nextID++
	p := &player{id: "p" + strconv.Itoa(h.nextID), name: cleanName(name), send: make(chan []byte, sendBuffer)}
	l := h.waiting[mode]
	if l == nil {
		l = &lobby{mode: mode, cap: capacity, deadline: time.Now().Add(lobbyWait)}
		h.waiting[mode] = l
	}
	l.players = append(l.players, p)
	p.lobby = l
	if len(l.players) >= l.cap {
		h.start(l)
	} else {
		l.sendStatus()
	}
	return p, true
}

func (h *hub) leave(p *player) {
	h.mu.Lock()
	defer h.mu.Unlock()
	close(p.send)
	l := p.lobby
	if l == nil {
		return
	}
	for i, q := range l.players {
		if q == p {
			l.players = append(l.players[:i], l.players[i+1:]...)
			break
		}
	}
	if !l.started {
		if len(l.players) == 0 {
			delete(h.waiting, l.mode)
		} else {
			l.sendStatus()
		}
		return
	}
	l.broadcast(encode(map[string]any{"t": "left", "id": p.id}), nil)
	if l.host == p && len(l.players) > 0 {
		l.host = l.players[0]
		l.broadcast(encode(map[string]any{"t": "host", "id": l.host.id}), nil)
	}
}

// relay forwards an in-match message, stamped with its sender, either to the
// player named in "to" or to everyone else in the lobby.
func (h *hub) relay(p *player, raw []byte) {
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	var t, to string
	json.Unmarshal(m["t"], &t)
	json.Unmarshal(m["to"], &to)

	h.mu.Lock()
	defer h.mu.Unlock()
	l := p.lobby
	if l == nil || !l.started {
		return
	}
	if t == "yield" {
		// The host hands the bots to the next player (e.g. its tab went to the background).
		if l.host == p && len(l.players) > 1 {
			for i, q := range l.players {
				if q == p {
					l.host = l.players[(i+1)%len(l.players)]
					break
				}
			}
			l.broadcast(encode(map[string]any{"t": "host", "id": l.host.id}), nil)
		}
		return
	}
	m["from"], _ = json.Marshal(p.id)
	msg := encode(m)
	if to != "" {
		if q := l.find(to); q != nil && q != p {
			q.deliver(msg)
		}
		return
	}
	l.broadcast(msg, p)
}

type queueMsg struct {
	T    string `json:"t"`
	Name string `json:"name"`
	Mode string `json:"mode"`
}

func (h *hub) serveWS(w http.ResponseWriter, req *http.Request) {
	conn, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetReadLimit(maxMsgBytes)
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	})

	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	var q queueMsg
	if err := conn.ReadJSON(&q); err != nil || q.T != "queue" {
		return
	}
	p, ok := h.queue(q.Name, q.Mode)
	if !ok {
		return
	}
	defer h.leave(p)

	go func() {
		ping := time.NewTicker(20 * time.Second)
		defer ping.Stop()
		for {
			select {
			case msg, ok := <-p.send:
				if !ok {
					return
				}
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if conn.WriteMessage(websocket.TextMessage, msg) != nil {
					conn.Close()
					return
				}
			case <-ping.C:
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if conn.WriteMessage(websocket.PingMessage, nil) != nil {
					conn.Close()
					return
				}
			}
		}
	}()

	for {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		h.relay(p, raw)
	}
}
