package main

import "fmt"
import "strings"

type userspecType struct {
	user     string
	password string
}

func (u *userspecType) finalize() string {
	if u.user == "" {
		return ""
	}

	if u.password == "" {
		return fmt.Sprintf("%s@", u.user)
	}

	return fmt.Sprintf("%s:%s@", u.user, u.password)
}

type hostspecType struct {
	host string
	port string
}

func (h *hostspecType) finalize() string {
	if h.port == "" {
		return h.host
	}
	return fmt.Sprintf("%s:%s", h.host, h.port)
}

type dbnameType string

func (d *dbnameType) finalize() string {
	if *d == "" {
		return ""
	}
	return fmt.Sprintf("/%s", *d)
}

type paramspecType []string

func (p *paramspecType) finalize() string {
	if len(*p) == 0 {
		return ""
	}
	return fmt.Sprintf("?%s", strings.Join(*p, "&"))
}

func buildURL(conf configSectionType) string {
	var dbname dbnameType
	userspec := userspecType{}
	hostspec := hostspecType{}
	paramspec := make(paramspecType, 0)
	for k, v := range conf {
		switch k {
		case "host":
			hostspec.host = v
		case "port":
			hostspec.port = v
		case "user":
			userspec.user = v
		case "password":
			userspec.password = v
		case "dbname":
			dbname = dbnameType(v)
		default:
			paramspec = append(paramspec, fmt.Sprintf("%s=%s", k, v))
		}
	}

	return strings.Join([]string{"postgresql://", userspec.finalize(), hostspec.finalize(), dbname.finalize(), paramspec.finalize()}, "")
}
