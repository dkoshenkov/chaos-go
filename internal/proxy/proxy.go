package proxy

import (
	"errors"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

// New returns a reverse proxy for the fixed upstream URL.
func New(upstream string) (http.Handler, error) {
	target, err := url.Parse(upstream)
	if err != nil {
		return nil, errors.New("invalid upstream URL")
	}
	if (target.Scheme != "http" && target.Scheme != "https") || target.Hostname() == "" {
		return nil, errors.New("upstream must be an absolute HTTP or HTTPS URL with a host")
	}
	if target.User != nil || strings.Contains(upstream, "#") {
		return nil, errors.New("upstream URL must not include user information or a fragment")
	}

	return &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			for name := range request.Out.Header {
				if strings.EqualFold(name, "Forwarded") || strings.HasPrefix(strings.ToLower(name), "x-forwarded-") {
					delete(request.Out.Header, name)
				}
			}
			request.SetURL(target)
			request.SetXForwarded()
		},
	}, nil
}
