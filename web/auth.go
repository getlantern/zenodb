package web

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	authcookie = "authcookie"
	xsrftoken  = "xsrftoken"
	authheader = "X-Zeno-Auth-Token"

	randomKeyLength = 32
)

var (
	sessionTimeout = 1 * time.Hour
)

type AuthData struct {
	AccessToken string
	Expiration  time.Time
}

func (h *handler) authenticate(resp http.ResponseWriter, req *http.Request) bool {
	if h.Opts.OAuthClientID == "" || h.Opts.OAuthClientSecret == "" {
		log.Debug("OAuth not configured, not authenticating!")
		return true
	}

	// First check for static auth token
	if h.Opts.Password != "" {
		password := req.Header.Get(authheader)
		if password != "" {
			result := password == h.Opts.Password
			return result
		}
	}

	// Then check for GitHub credentials
	cookie, err := req.Cookie(authcookie)
	if err == nil {
		ad := &AuthData{}
		err = h.sc.Decode(authcookie, cookie.Value, ad)
		if err == nil {
			if ad.Expiration.Before(time.Now()) {
				return true
			}
			inOrg, err := h.userInOrg(ad.AccessToken)
			if err != nil {
				log.Errorf("Unable to check if user is in org: %v", err)
			} else if inOrg {
				ad.Expiration = time.Now().Add(sessionTimeout)
				return true
			}
		}
	}

	// User not logged in, request authorization from OAuth provider
	h.requestAuthorization(resp, req)

	return false
}

func (h *handler) requestAuthorization(resp http.ResponseWriter, req *http.Request) {
	xsrfExpiration := time.Now().Add(1 * time.Minute)
	state, err := h.sc.Encode(xsrftoken, xsrfExpiration)
	if err != nil {
		log.Errorf("Unable to encode xsrf token: %v", err)
		// TODO: figure out how to handle this
		return
	}

	u, err := buildURL("https://github.com/login/oauth/authorize", map[string]string{
		"client_id": h.OAuthClientID,
		"state":     state,
		"scope":     "read:org",
	})
	if err != nil {
		panic(err)
	}

	log.Debugf("Redirecting to: %v", u.String())

	resp.Header().Set("Location", u.String())
	resp.WriteHeader(http.StatusTemporaryRedirect)
}

func (h *handler) oauthCode(resp http.ResponseWriter, req *http.Request) {
	code := req.URL.Query().Get("code")
	state := req.URL.Query().Get("state")
	var xsrfExpiration time.Time
	err := h.sc.Decode(xsrftoken, state, &xsrfExpiration)
	if err != nil {
		log.Errorf("Unable to decode xsrf token, may indicate attempted attack, re-authorizing: %v", err)
		h.requestAuthorization(resp, req)
	}
	if time.Now().After(xsrfExpiration) {
		log.Error("XSRF Token expired, re-authorizing")
		h.requestAuthorization(resp, req)
		return
	}

	u, err := buildURL("https://github.com/login/oauth/access_token", map[string]string{
		"client_id":     h.OAuthClientID,
		"client_secret": h.OAuthClientSecret,
		"code":          code,
		"state":         state,
	})
	if err != nil {
		panic(err)
	}

	post, _ := http.NewRequest(http.MethodPost, u.String(), nil)
	post.Header.Set("Accept", "application/json")
	tokenResp, err := h.client.Do(post)
	if err != nil {
		log.Errorf("Error requesting access token, re-authorizing: %v", err)
		h.requestAuthorization(resp, req)
		return
	}

	defer tokenResp.Body.Close()
	body, err := ioutil.ReadAll(tokenResp.Body)
	if err != nil {
		log.Errorf("Error reading access token, re-authorizing: %v", err)
		h.requestAuthorization(resp, req)
		return
	}

	tokenData := make(map[string]string)
	err = json.Unmarshal(body, &tokenData)
	if err != nil {
		log.Errorf("Error unmarshalling access token, re-authorizing: %v", err)
		h.requestAuthorization(resp, req)
		return
	}

	accessToken := tokenData["access_token"]
	inOrg, err := h.userInOrg(accessToken)
	if err != nil {
		log.Errorf("Unable to check if user is in org, re-authorizing: %v", err)
	} else if !inOrg {
		log.Errorf("User not in needed org")
		// TODO: figure out what to do
		return
	}

	ad := &AuthData{
		AccessToken: accessToken,
		Expiration:  time.Now().Add(sessionTimeout),
	}
	cookieData, err := h.sc.Encode(authcookie, ad)
	if err != nil {
		log.Errorf("Unable to encode authcookie: %v", err)
		// TODO: figure out what to handle here
		return
	}
	http.SetCookie(resp, &http.Cookie{
		Path:    "/",
		Secure:  true,
		Name:    authcookie,
		Value:   cookieData,
		Expires: time.Now().Add(365 * 24 * time.Hour),
	})

	log.Debug("User logged in!")
	resp.Header().Set("Location", "/")
	resp.WriteHeader(http.StatusTemporaryRedirect)
}

func (h *handler) userInOrg(accessToken string) (bool, error) {
	req, _ := http.NewRequest(http.MethodGet, "https://api.github.com/user/orgs", nil)
	req.Header.Set("Authorization", fmt.Sprintf("token %v", accessToken))
	resp, err := h.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("Unable to get user orgs from GitHub: %v", err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("Unable to read user orgs from GitHub: %v", err)
	}
	if resp.StatusCode > 299 {
		return false, fmt.Errorf("Got response status %d: %v", resp.StatusCode, string(body))
	}
	orgs := make([]map[string]interface{}, 0)
	err = json.Unmarshal(body, &orgs)
	if err != nil {
		if err != nil {
			return false, fmt.Errorf("Unable to unmarshal user orgs from GitHub: %v", err)
		}
	}

	for _, org := range orgs {
		if org["login"] == h.GitHubOrg {
			return true, nil
		}
	}

	log.Debugf("User not in org %v", h.GitHubOrg)
	return false, nil
}
