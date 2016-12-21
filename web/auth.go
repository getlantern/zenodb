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

	randomKeyLength = 32
)

var (
	sessionTimeout = 1 * time.Hour
)

type AuthData struct {
	AccessToken string
	Expiration  time.Time
}

func (s *server) authenticate(resp http.ResponseWriter, req *http.Request) bool {
	cookie, err := req.Cookie(authcookie)
	if err == nil {
		ad := &AuthData{}
		err = s.sc.Decode(authcookie, cookie.Value, ad)
		if err == nil {
			if ad.Expiration.Before(time.Now()) {
				return true
			}
			inOrg, err := s.userInOrg(ad.AccessToken)
			if err != nil {
				log.Errorf("Unable to check if user is in org: %v", err)
			} else if inOrg {
				ad.Expiration = time.Now().Add(sessionTimeout)
				return true
			}
		}
	}

	// User not logged in, request authorization from OAuth provider
	s.requestAuthorization(resp, req)

	return false
}

func (s *server) requestAuthorization(resp http.ResponseWriter, req *http.Request) {
	xsrfExpiration := time.Now().Add(1 * time.Minute)
	state, err := s.sc.Encode(xsrftoken, xsrfExpiration)
	if err != nil {
		log.Errorf("Unable to encode xsrf token: %v", err)
		// TODO: figure out how to handle this
		return
	}

	u, err := buildURL("https://github.com/login/oauth/authorize", map[string]string{
		"client_id": s.OAuthClientID,
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

func (s *server) oauthCode(resp http.ResponseWriter, req *http.Request) {
	code := req.URL.Query().Get("code")
	state := req.URL.Query().Get("state")
	var xsrfExpiration time.Time
	err := s.sc.Decode(xsrftoken, state, &xsrfExpiration)
	if err != nil {
		log.Errorf("Unable to decode xsrf token, may indicate attempted attack, re-authorizing: %v", err)
		s.requestAuthorization(resp, req)
	}
	if time.Now().After(xsrfExpiration) {
		log.Error("XSRF Token expired, re-authorizing")
		s.requestAuthorization(resp, req)
		return
	}

	u, err := buildURL("https://github.com/login/oauth/access_token", map[string]string{
		"client_id":     s.OAuthClientID,
		"client_secret": s.OAuthClientSecret,
		"code":          code,
		"state":         state,
	})
	if err != nil {
		panic(err)
	}

	post, _ := http.NewRequest(http.MethodPost, u.String(), nil)
	post.Header.Set("Accept", "application/json")
	tokenResp, err := s.client.Do(post)
	if err != nil {
		log.Errorf("Error requesting access token, re-authorizing: %v", err)
		s.requestAuthorization(resp, req)
		return
	}

	defer tokenResp.Body.Close()
	body, err := ioutil.ReadAll(tokenResp.Body)
	if err != nil {
		log.Errorf("Error reading access token, re-authorizing: %v", err)
		s.requestAuthorization(resp, req)
		return
	}

	tokenData := make(map[string]string)
	err = json.Unmarshal(body, &tokenData)
	if err != nil {
		log.Errorf("Error unmarshalling access token, re-authorizing: %v", err)
		s.requestAuthorization(resp, req)
		return
	}

	accessToken := tokenData["access_token"]
	inOrg, err := s.userInOrg(accessToken)
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
	cookieData, err := s.sc.Encode(authcookie, ad)
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

func (s *server) userInOrg(accessToken string) (bool, error) {
	req, _ := http.NewRequest(http.MethodGet, "https://api.github.com/user/orgs", nil)
	req.Header.Set("Authorization", fmt.Sprintf("token %v", accessToken))
	resp, err := s.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("Unable to get user orgs from GitHub: %v", err)
	}
	defer resp.Body.Close()
	orgsData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("Unable to read user orgs from GitHub: %v", err)
	}
	orgs := make([]map[string]interface{}, 0)
	err = json.Unmarshal(orgsData, &orgs)
	if err != nil {
		if err != nil {
			return false, fmt.Errorf("Unable to unmarshal user orgs from GitHub: %v", err)
		}
	}

	for _, org := range orgs {
		if org["login"] == s.GitHubOrg {
			return true, nil
		}
	}

	log.Debugf("User not in org %v", s.GitHubOrg)
	return false, nil
}
