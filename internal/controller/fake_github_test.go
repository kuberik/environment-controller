package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
)

// fakeGitHub implements the handful of GitHub Deployments endpoints the
// reconciler uses, in memory. It exists so wave tests never touch real GitHub:
// the other clusters of a fleet are simulated by seeding their deployments and
// statuses directly.
const stateSuccess = "success"

type fakeGitHub struct {
	mu     sync.Mutex
	srv    *httptest.Server
	nextID int64
	deps   []*fakeDep // creation order
	calls  []string   // "METHOD path"
}

type fakeDep struct {
	ID          int64
	Ref         string
	Task        string
	Environment string
	Description string
	Payload     json.RawMessage
	Statuses    []fakeStatus // oldest first
}

type fakeStatus struct {
	ID             int64
	State          string
	Description    string
	Environment    string
	EnvironmentURL string
	AutoInactive   *bool
}

func newFakeGitHub() *fakeGitHub {
	f := &fakeGitHub{nextID: 1000}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	return f
}

func (f *fakeGitHub) Close()      { f.srv.Close() }
func (f *fakeGitHub) URL() string { return f.srv.URL }

func (f *fakeGitHub) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeGitHub) countCalls(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

// addDeployment seeds a deployment (payload is marshalled as a JSON object).
func (f *fakeGitHub) addDeployment(task, env, ref string, payload any) int64 {
	raw, _ := json.Marshal(payload)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	f.deps = append(f.deps, &fakeDep{ID: f.nextID, Ref: ref, Task: task, Environment: env, Payload: raw})
	return f.nextID
}

func (f *fakeGitHub) addStatus(depID int64, state, desc string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addStatusLocked(f.find(depID), fakeStatus{State: state, Description: desc})
}

func (f *fakeGitHub) find(id int64) *fakeDep {
	for _, d := range f.deps {
		if d.ID == id {
			return d
		}
	}
	return nil
}

func (f *fakeGitHub) addStatusLocked(d *fakeDep, s fakeStatus) {
	f.nextID++
	s.ID = f.nextID
	d.Statuses = append(d.Statuses, s)
	// GitHub default: a success marks older deployments of the same environment
	// inactive unless auto_inactive=false.
	if s.State == stateSuccess && (s.AutoInactive == nil || *s.AutoInactive) {
		for _, o := range f.deps {
			if o.ID < d.ID && o.Environment == d.Environment && len(o.Statuses) > 0 {
				f.nextID++
				o.Statuses = append(o.Statuses, fakeStatus{ID: f.nextID, State: "inactive"})
			}
		}
	}
}

// deploymentsOf returns copies of deployments of a GitHub environment.
func (f *fakeGitHub) deploymentsOf(env string) []fakeDep {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakeDep
	for _, d := range f.deps {
		if d.Environment == env {
			cp := *d
			cp.Statuses = append([]fakeStatus(nil), d.Statuses...)
			out = append(out, cp)
		}
	}
	return out
}

func (d *fakeDep) json() map[string]any {
	var payload any
	_ = json.Unmarshal(d.Payload, &payload)
	return map[string]any{"id": d.ID, "ref": d.Ref, "sha": d.Ref, "task": d.Task, "environment": d.Environment, "description": d.Description, "payload": payload}
}

func (s fakeStatus) json() map[string]any {
	return map[string]any{"id": s.ID, "state": s.State, "description": s.Description, "environment": s.Environment, "environment_url": s.EnvironmentURL}
}

func (f *fakeGitHub) paginate(w http.ResponseWriter, r *http.Request, n int) (from, to int) {
	per, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	if per <= 0 {
		per = 30
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page <= 0 {
		page = 1
	}
	from, to = (page-1)*per, page*per
	if from > n {
		from = n
	}
	if to > n {
		to = n
	}
	if to < n {
		q := r.URL.Query()
		q.Set("page", strconv.Itoa(page+1))
		w.Header().Set("Link", fmt.Sprintf("<http://%s%s?%s>; rel=\"next\"", r.Host, r.URL.Path, q.Encode()))
	}
	return from, to
}

func (f *fakeGitHub) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, r.Method+" "+r.URL.Path)

	// /repos/{owner}/{repo}/deployments[/{id}[/statuses]]
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 4 || parts[0] != "repos" || parts[3] != "deployments" {
		http.NotFound(w, r)
		return
	}
	out := func(code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	switch {
	case len(parts) == 4 && r.Method == http.MethodGet:
		q := r.URL.Query()
		var matched []*fakeDep
		for i := len(f.deps) - 1; i >= 0; i-- { // newest first
			d := f.deps[i]
			if (q.Get("task") != "" && d.Task != q.Get("task")) ||
				(q.Get("environment") != "" && d.Environment != q.Get("environment")) ||
				(q.Get("ref") != "" && d.Ref != q.Get("ref")) {
				continue
			}
			matched = append(matched, d)
		}
		from, to := f.paginate(w, r, len(matched))
		res := []map[string]any{}
		for _, d := range matched[from:to] {
			res = append(res, d.json())
		}
		out(200, res)
	case len(parts) == 4 && r.Method == http.MethodPost:
		var req struct {
			Ref, Task, Environment, Description string
			Payload                             json.RawMessage
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		f.nextID++
		d := &fakeDep{ID: f.nextID, Ref: req.Ref, Task: req.Task, Environment: req.Environment, Description: req.Description, Payload: req.Payload}
		f.deps = append(f.deps, d)
		out(201, d.json())
	case len(parts) == 5 && r.Method == http.MethodGet:
		id, _ := strconv.ParseInt(parts[4], 10, 64)
		if d := f.find(id); d != nil {
			out(200, d.json())
			return
		}
		http.NotFound(w, r)
	case len(parts) == 6 && parts[5] == "statuses":
		id, _ := strconv.ParseInt(parts[4], 10, 64)
		d := f.find(id)
		if d == nil {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodGet {
			n := len(d.Statuses)
			from, to := f.paginate(w, r, n)
			res := []map[string]any{}
			for i := n - 1 - from; i > n-1-to; i-- { // newest first
				res = append(res, d.Statuses[i].json())
			}
			out(200, res)
			return
		}
		var req struct {
			State          string `json:"state"`
			Description    string `json:"description"`
			Environment    string `json:"environment"`
			EnvironmentURL string `json:"environment_url"`
			AutoInactive   *bool  `json:"auto_inactive"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		s := fakeStatus{State: req.State, Description: req.Description, Environment: req.Environment, EnvironmentURL: req.EnvironmentURL, AutoInactive: req.AutoInactive}
		f.addStatusLocked(d, s)
		out(201, d.Statuses[len(d.Statuses)-1].json())
	default:
		http.NotFound(w, r)
	}
}
