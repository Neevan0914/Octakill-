package main

import (
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Online Free For All. The server keeps rooms of up to maxPlayers, relays
// movement/shots/effects between players and keeps the scoreboard. Each
// client is authoritative for its own health: shooters send "hit" to their
// victim, and a victim that dies reports "died" so the server can score it.

const (
	maxPlayers   = 8 // the game has 7 opponent models + yourself
	killTarget   = 15
	roundLength  = 180 * time.Second
	maxMsgBytes  = 4096
	maxHitDamage = 250
	sendBuffer   = 256
)

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		// The game page may be hosted elsewhere (GitHub Pages, itch.io, ...).
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	roomNameRe = regexp.MustCompile(`[^a-z0-9_-]`)
	colorRe    = regexp.MustCompile(`^#[0-9a-fA-F]{3,8}$`)
	mapIDRe    = regexp.MustCompile(`^[a-z]{1,16}$`)
)

type player struct {
	id     string
	name   string
	kills  int
	deaths int
	send   chan []byte
	room   *room
}

type room struct {
	name     string
	mapID    string
	roundEnd time.Time
	players  map[string]*player
	order    []string // join order, used for stable scoreboards
	nextID   int
}

type hub struct {
	mu    sync.Mutex
	rooms map[string]*room
}

func newHub() *hub {
	h := &hub{rooms: map[string]*room{}}
	go h.roundTimer()
	return h
}

type scoreRow struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Kills  int    `json:"k"`
	Deaths int    `json:"d"`
}

func (r *room) scores() []scoreRow {
	rows := make([]scoreRow, 0, len(r.order))
	for _, id := range r.order {
		p := r.players[id]
		rows = append(rows, scoreRow{p.id, p.name, p.kills, p.deaths})
	}
	return rows
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
	default: // client too slow; drop rather than stall the room
	}
}

// broadcast must be called with h.mu held.
func (r *room) broadcast(msg []byte, except *player) {
	for _, p := range r.players {
		if p != except {
			p.deliver(msg)
		}
	}
}

func (r *room) resetRound() {
	r.roundEnd = time.Now().Add(roundLength)
	for _, p := range r.players {
		p.kills, p.deaths = 0, 0
	}
}

// endRound must be called with h.mu held.
func (r *room) endRound(winner string) {
	r.broadcast(encode(map[string]any{"t": "end", "w": winner, "players": r.scores()}), nil)
	r.resetRound()
}

func (r *room) leader() string {
	best, bestKills := "", -1
	for _, id := range r.order {
		if p := r.players[id]; p.kills > bestKills {
			best, bestKills = id, p.kills
		}
	}
	return best
}

func (h *hub) roundTimer() {
	for range time.Tick(time.Second) {
		h.mu.Lock()
		for _, r := range h.rooms {
			if len(r.players) > 0 && time.Now().After(r.roundEnd) {
				r.endRound(r.leader())
			}
		}
		h.mu.Unlock()
	}
}

type joinMsg struct {
	Name string `json:"name"`
	Room string `json:"room"`
	Map  string `json:"map"`
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

func cleanRoom(s string) string {
	s = roomNameRe.ReplaceAllString(strings.ToLower(s), "")
	if len(s) > 24 {
		s = s[:24]
	}
	if s == "" {
		s = "public"
	}
	return s
}

func (h *hub) join(j joinMsg) (*player, []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	name := cleanRoom(j.Room)
	r := h.rooms[name]
	if r == nil {
		mapID := j.Map
		if !mapIDRe.MatchString(mapID) {
			mapID = "arena"
		}
		r = &room{name: name, mapID: mapID, players: map[string]*player{}}
		h.rooms[name] = r
	}
	if len(r.players) >= maxPlayers {
		return nil, encode(map[string]any{"t": "full"})
	}
	if len(r.players) == 0 {
		r.resetRound()
	}
	r.nextID++
	p := &player{id: "p" + strconv.Itoa(r.nextID), name: cleanName(j.Name), send: make(chan []byte, sendBuffer), room: r}
	r.broadcast(encode(map[string]any{"t": "join", "id": p.id, "name": p.name}), nil)
	r.players[p.id] = p
	r.order = append(r.order, p.id)
	welcome := encode(map[string]any{
		"t": "welcome", "id": p.id, "map": r.mapID, "room": r.name,
		"time": int(time.Until(r.roundEnd).Seconds()), "target": killTarget, "players": r.scores(),
	})
	log.Printf("room %s: %s (%s) joined, %d players", r.name, p.id, p.name, len(r.players))
	return p, welcome
}

func (h *hub) leave(p *player) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := p.room
	delete(r.players, p.id)
	for i, id := range r.order {
		if id == p.id {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
	close(p.send)
	r.broadcast(encode(map[string]any{"t": "leave", "id": p.id}), nil)
	if len(r.players) == 0 {
		delete(h.rooms, r.name)
	}
	log.Printf("room %s: %s left, %d players", r.name, p.id, len(r.players))
}

// clientMsg is the union of everything a client may send after joining.
type clientMsg struct {
	T string `json:"t"`
	// state
	P [3]float64 `json:"p"`
	R float64    `json:"r"`
	C bool       `json:"c"`
	A bool       `json:"a"`
	// shot / effects
	From [3]float64 `json:"a3"`
	To   [3]float64 `json:"b3"`
	Col  string     `json:"col"`
	K    string     `json:"k"`
	Rad  float64    `json:"rad"`
	Dur  float64    `json:"dur"`
	// hit / died
	Target string `json:"to"`
	Dmg    int    `json:"d"`
	By     string `json:"by"`
}

func (h *hub) handle(p *player, m clientMsg) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := p.room
	switch m.T {
	case "s":
		r.broadcast(encode(map[string]any{"t": "s", "id": p.id, "p": m.P, "r": m.R, "c": m.C, "a": m.A}), p)
	case "shot":
		if !colorRe.MatchString(m.Col) {
			m.Col = "#f2c94c"
		}
		r.broadcast(encode(map[string]any{"t": "shot", "id": p.id, "a3": m.From, "b3": m.To, "col": m.Col}), p)
	case "fx":
		if m.K != "boom" && m.K != "smoke" && m.K != "flash" {
			return
		}
		if !colorRe.MatchString(m.Col) {
			m.Col = "#ff7a3d"
		}
		r.broadcast(encode(map[string]any{"t": "fx", "id": p.id, "k": m.K, "p": m.P, "rad": m.Rad, "dur": m.Dur, "col": m.Col}), p)
	case "hit":
		target := r.players[m.Target]
		if target == nil || target == p || m.Dmg <= 0 {
			return
		}
		if m.Dmg > maxHitDamage {
			m.Dmg = maxHitDamage
		}
		target.deliver(encode(map[string]any{"t": "hit", "from": p.id, "d": m.Dmg}))
	case "died":
		p.deaths++
		killer := r.players[m.By]
		if killer == p {
			killer = nil
		}
		killerID := ""
		if killer != nil {
			killer.kills++
			killerID = killer.id
		}
		r.broadcast(encode(map[string]any{"t": "kill", "k": killerID, "v": p.id, "players": r.scores()}), nil)
		if killer != nil && killer.kills >= killTarget {
			r.endRound(killer.id)
		}
	}
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
	var j joinMsg
	if err := conn.ReadJSON(&j); err != nil {
		return
	}
	p, reply := h.join(j)
	if p == nil {
		conn.WriteMessage(websocket.TextMessage, reply)
		return
	}
	p.deliver(reply)
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
		var m clientMsg
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		h.handle(p, m)
	}
}
