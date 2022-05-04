package kt

import (
	"embed"
	"io/fs"
	"log"
	"net/http"
	"path"
	"strings"
	"text/template"

	"github.com/AndrewBurian/eventsource"
)

//go:embed web
var web embed.FS

var webRoot = func() fs.FS {
	sub, err := fs.Sub(web, "web")
	if err != nil {
		log.Fatal(err)
	}
	return sub
}()

var webTemplate = func() *template.Template {
	subTemplate, err := template.New("").ParseFS(webRoot, "*.html")
	if err != nil {
		log.Fatal(err)
	}

	return subTemplate
}()

func SSEWebHandler(contextPath string, stream *eventsource.Stream) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		p := path.Join("/", strings.TrimPrefix(r.URL.Path, contextPath))
		if contextPath == "/" {
			contextPath = ""
		}

		switch p {
		case "/":
			if err := webTemplate.ExecuteTemplate(w, "index.html", map[string]string{
				"ContextPath": contextPath,
			}); err != nil {
				log.Fatal(err)
			}
		case "/sse":
			SSEHandler(stream).ServeHTTP(w, r)
		default:
			http.StripPrefix(contextPath, http.FileServer(http.FS(webRoot))).ServeHTTP(w, r)
		}
	}
}

type SSESender struct {
	Stream *eventsource.Stream
}

func (s *SSESender) Send(msg string) {
	s.Stream.Broadcast(eventsource.DataEvent(msg))
}

func (s *SSESender) Close() error {
	s.Stream.Shutdown()
	return nil
}

func NewSSEStream() *eventsource.Stream {
	return eventsource.NewStream()
}

func SSEHandler(stream *eventsource.Stream) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		stream.ServeHTTP(w, r)

		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
}
