package main

import (
	"embed"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

//go:embed web/*
var webFiles embed.FS

const (
	writeWait = 10 * time.Second
	pongWait = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	maxMessageSize = 4 * 1024
)

type Message struct {
	Type string `json:"type"`
	Room string `json:"room"`
	Username string `json:"username"`
	Text string `json:"text"`
	Timestamp time.Time `json:"timestamp"`
}

type Client struct {
	hub *Hub
	conn *websocket.Conn
	send chan Message
	username string
	room string
}

type Hub struct {
	mu sync.RWMutex
	rooms map[string]map[*Client]bool
	register chan *Client
	unregister chan *Client
	broadcast chan Message
}

func NewHub() *Hub {
	return &Hub{
		rooms: make(map[string]map[*Client]bool),
		register: make(chan *Client),
		unregister: make(chan *Client),
		broadcast: make(chan Message, 256),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.mu.Lock()
			if h.rooms[c.room] == nil {
				h.rooms[c.room] = make(map[*Client]bool)
			}
			h.rooms[c.room][c] = true
			h.mu.Unlock()
			h.broadcast <- Message{Type: "system", Room: c.room, Username: c.username, Text: c.username + " bergabung ke room.", Timestamp: time.Now().UTC()}

		case c := <-h.unregister:
			h.mu.Lock()
			if clients, ok := h.rooms[c.room]; ok {
				if _, exists := clients[c]; exists {
					delete(clients, c)
					close(c.send)
				}
				if len(clients) == 0 {
					delete(h.rooms, c.room)
				}
			}
			h.mu.Unlock()
			h.broadcast <- Message{Type: "system", Room: c.room, Username: c.username, Text: c.username + " keluar dari room.", Timestamp: time.Now().UTC()}

		case msg := <-h.broadcast:
			h.mu.RLock()
			for c := range h.rooms[msg.Room] {
				select {
				case c.send <- msg:
				default:
					go func(client *Client) { h.unregister <- client }(c)
				}
			}
			h.mu.RUnlock()
		}
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize: 1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" { return true }
		return strings.EqualFold(origin, "https://"+r.Host) || strings.EqualFold(origin, "http://"+r.Host)
	},
}

func clean(value, fallback string, max int) string {
	value = strings.TrimSpace(value)
	if value == "" { return fallback }
	runes := []rune(value)
	if len(runes) > max { return string(runes[:max]) }
	return value
}

func serveWS(hub *Hub, w http.ResponseWriter, r *http.Request) {
	username := clean(r.URL.Query().Get("username"), "Tamu", 24)
	room := clean(r.URL.Query().Get("room"), "umum", 32)
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade: %v", err)
		return
	}
	client := &Client{hub: hub, conn: conn, send: make(chan Message, 32), username: username, room: room}
	client.hub.register <- client
	go client.writePump()
	client.readPump()
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		_ = c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error { return c.conn.SetReadDeadline(time.Now().Add(pongWait)) })

	for {
		var incoming struct { Text string `json:"text"` }
		if err := c.conn.ReadJSON(&incoming); err != nil { break }
		text := clean(incoming.Text, "", 1000)
		if text == "" { continue }
		c.hub.broadcast <- Message{Type: "message", Room: c.room, Username: c.username, Text: text, Timestamp: time.Now().UTC()}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() { ticker.Stop(); _ = c.conn.Close() }()
	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok { _ = c.conn.WriteMessage(websocket.CloseMessage, []byte{}); return }
			if err := c.conn.WriteJSON(msg); err != nil { return }
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil { return }
		}
	}
}

func main() {
	hub := NewHub()
	go hub.Run()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" { http.NotFound(w, r); return }
		http.ServeFileFS(w, r, webFiles, "web/index.html")
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet { http.Error(w, "Method not allowed", http.StatusMethodNotAllowed); return }
		serveWS(hub, w, r)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status":"ok"})
	})

	port := clean(os.Getenv("PORT"), "8080", 10)
	addr := "0.0.0.0:" + port
	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("Chat server listening on %s", addr)
	log.Fatal(server.ListenAndServe())
}
