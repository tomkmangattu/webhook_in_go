package main


import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
	"bytes"
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type Status string

const (
	StatusOpen       Status = "open"
	StatusClosed     Status = "closed"
	StatusInProgress Status = "in_progress"
)

type Ticket struct {
	ID 			int `json:"id"`
	Title 		string `json:"title"`
	Description string `json:"description"`
	Status 		Status `json:"status"`
	CreatedAt 	time.Time `json:"created_at"`
	UpdatedAt 	time.Time `json:"updated_at"`
}

type WebhookEvent struct {
	Event string `json:"event"`
	Ticket Ticket `json:"ticket"`
	Timestamp time.Time `json:"timestamp"`
}

type Webhook struct {
	ID int `json:"id"`
	URL string `json:"url"`
}

type WebhookStore struct {
	mu sync.RWMutex
	webhooks map[int]Webhook
	nextID int
}

func NewWebhookStore() *WebhookStore {
	return &WebhookStore{
		webhooks: make(map[int]Webhook),
		nextID: 1,
	}
}

func (s *WebhookStore) Add(url string) Webhook {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := Webhook{ ID: s.nextID, URL: url}
	s.webhooks[s.nextID] = w
	s.nextID++
	return w
}

func (s *WebhookStore) List() []Webhook {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Webhook, 0, len(s.webhooks))
	for _, w := range s.webhooks {
		result = append(result, w)
	}
	return result
}

func (s *WebhookStore) Delete(id int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.webhooks[id]; !ok {
		return false
	}
	delete(s.webhooks, id)
	return true
}

const kafkaTopic = "ticket_events"
const kafkaBroker = "localhost:9092"

func publishTicketEvent(ticket Ticket) error {
	write := &kafka.Writer{
		Addr: kafka.TCP(kafkaBroker),
		Topic: kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer write.Close()

	event := WebhookEvent{
		Event: "ticket.created",
		Ticket: ticket,
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return write.WriteMessages(context.Background(), kafka.Message{
		Key : []byte(fmt.Sprintf("ticket-%d", ticket.ID)),
		Value: payload,
	})
}

func startWebhookConsumer(ctx context.Context, webhookStore *WebhookStore) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic: kafkaTopic,
		GroupID: "webhook-dispatcher",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	go func() {
		defer reader.Close()
		log.Println("Webhook consumer started, listening for events...")
		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				return
			}
			log.Printf("Received event: %s\n", string(msg.Value))

			for _, webhook := range webhookStore.List() {
				go dispatchWebhook(webhook.URL, msg.Value)
			}
		}
	}()
}

func dispatchWebhook(url string, payload []byte){
	client := &http.Client{ Timeout: 10 * time.Second }
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		log.Printf("Failed to dispatch webhook to %s: %v\n", url, err)
		return
	}
	defer resp.Body.Close()
	log.Printf("Dispatched webhook to %s, response status: %s\n", url, resp.Status)
}

type TicketStore struct {
	mu sync.RWMutex
	tickets map[int]Ticket
	nextID  int
}

func NewTicketStore() *TicketStore {
	return &TicketStore{
		tickets: make(map[int]Ticket),
		nextID:  1,
	}
}

func (s *TicketStore) Create(title, description string) Ticket {
	s.mu.Lock()
	defer s.mu.Unlock()

	t := Ticket{
		ID:          s.nextID,
		Title:       title,
		Description: description,
		Status:      StatusOpen,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	s.tickets[s.nextID] = t
	s.nextID++
	return t
}

func (s *TicketStore) List() []Ticket {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]Ticket, 0, len(s.tickets))
	for _, t := range s.tickets {
		result = append(result, t)
	}
	return result
}

func (s *TicketStore) Get(id int) (Ticket, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.tickets[id]
	return t, ok
}

func (s *TicketStore) UpdateStatus(id int, status Status) (Ticket, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tickets[id]
	if !ok {
		return Ticket{}, false
	}

	t.Status = status
	t.UpdatedAt = time.Now()
	s.tickets[id] = t
	return t, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func main() {
	store := NewTicketStore()
	mux := http.NewServeMux()
	webhookStore := NewWebhookStore()

	mux.HandleFunc("POST /webhooks", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		if req.URL == "" {
			http.Error(w, "URL is required", http.StatusBadRequest)
			return
		}
		wh := webhookStore.Add(req.URL)
		writeJSON(w, http.StatusCreated, wh)
	})

	mux.HandleFunc("GET /webhooks", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, webhookStore.List())
	})

	mux.HandleFunc("DELETE /webhooks/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := 0
		fmt.Sscanf(r.PathValue("id"), "%d", &id)
		if !webhookStore.Delete(id) {
			http.Error(w, "Webhook not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startWebhookConsumer(ctx, webhookStore)


	// Serve frontend
	mux.Handle("GET /", http.FileServer(http.Dir("static")))

	mux.HandleFunc("GET /tickets", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, store.List())
	})

	mux.HandleFunc("POST /tickets", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Title       string `json:"title"`
			Description string `json:"description"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		if req.Title == "" {
			http.Error(w, "Title is required", http.StatusBadRequest)
			return
		}
		
		t := store.Create(req.Title, req.Description)

		go func () {
			if err := publishTicketEvent(t); err != nil {
				fmt.Printf("Failed to publish ticket event: %v\n", err)
			}
		}()

		writeJSON(w, http.StatusCreated, t)
	})

	mux.HandleFunc("GET /tickets/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := 0
		fmt.Sscanf(r.PathValue("id"), "%d", &id)
		t, ok := store.Get(id)
		if !ok {
			http.Error(w, "Ticket not found", http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, t)
	})
	
	mux.HandleFunc("PATCH /tickets/{id}", func(w http.ResponseWriter, r *http.Request){
		id := 0
		fmt.Sscanf(r.PathValue("id"), "%d", &id)

		var req struct {
			Status Status `json:"status"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		switch req.Status {
		case StatusOpen, StatusClosed, StatusInProgress:
		default:
			http.Error(w, "Invalid status (open, in progress, closed)", http.StatusBadRequest)
			return
		}

		t, ok := store.UpdateStatus(id, req.Status)
		if !ok {
			http.Error(w, "Ticket not found", http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, t)
	})

	fmt.Println("Server running on http://localhost:8686")
	http.ListenAndServe(":8686", mux)
}
