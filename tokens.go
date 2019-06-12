// ESI Token Management

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

type token struct {
	CharacterID  uint64   `json:"character_id"`
	CorpID       uint64   `json:"corp_id"`
	RefreshToken string   `json:"refresh_token"`
	Scopes       []string `json:"scopes"`
	AccessToken  string   `json:"access_token"`
	Expires      int64    `json:"expires"`
}
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
	// RefreshToken string `json:"refresh_token"`
}
type verifyResponse struct {
	CharacterID   uint64 `json:"CharacterID"`
	CharacterName string `json:"CharacterName"`
	// ExpiresOn            string `json:"ExpiresOn"`
	Scopes string `json:"Scopes"`
	// TokenType            string `json:"TokenType"`
	// CharacterOwnerHash   string `json:"CharacterOwnerHash"`
	// IntellectualProperty string `json:"IntellectualProperty"`
}

var tokens map[uint64]*token

func tokensInit() {
	res := safeQuery(fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` WHERE LENGTH(refresh_token) > 4", c.Tables["chars"].DB, c.Tables["chars"].Name))
	defer res.Close()
	if !res.Next() {
		tokens = make(map[uint64]*token)
		return
	}
	var numRecords int
	res.Scan(&numRecords)
	tokens = make(map[uint64]*token, numRecords)

	ress := safeQuery(fmt.Sprintf("SELECT id,corpID,refresh_token,access_token,access_token_exp,scopes FROM `%s`.`%s` WHERE LENGTH(refresh_token) > 4", c.Tables["chars"].DB, c.Tables["chars"].Name))
	defer ress.Close()
	for ress.Next() {
		tok := token{}
		var poo string
		ress.Scan(&tok.CharacterID, &tok.CorpID, &tok.RefreshToken, &tok.AccessToken, &tok.Expires, &poo)
		tok.Scopes = strings.Split(poo, " ")
		tokens[tok.CharacterID] = &tok

	}

}

func encodeAuthToken(clientID string, clientSecret string) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", clientID, clientSecret)))
	// fmt.Printf("base 64 encoded: %s\n", encodeAuthToken(c.Oauth["CCP"].ClientID, c.Oauth["CCP"].ClientSecret))
}
func decodeAuthToken(authToken string) (clientID string, clientSecret string) {
	out, _ := base64.StdEncoding.DecodeString(authToken)
	foo := strings.Split(string(out), ":")
	return foo[0], foo[1]
	// clientID, clientSecret := decodeAuthToken(c.Oauth["CCP"].AuthToken)
	// fmt.Printf("base 64 decoded: clientID: %s, clientSecret: %s\n", clientID, clientSecret)
}
func getAccessToken(characterID uint64, scope string) (accessToken string) {
	tok, ok := tokens[characterID]
	if !ok {
		logf("no token data on file for %d", characterID)
		return ""
	}
	if len(scope) > 0 {
		valid := false
		for it := range tok.Scopes {
			if tok.Scopes[it] == scope {
				valid = true
				break
			}
		}
		if !valid {
			logf("token lacks required scope %d", characterID)
			return ""
		}
	}
	if tok.Expires > ktime() {
		// logf("cached access_token returned for %d", characterID)
		return tok.AccessToken
	}
	data := []byte(fmt.Sprintf("grant_type=refresh_token&refresh_token=%s", tok.RefreshToken))

	req, err := http.NewRequest("POST", c.Oauth["CCP"].TokenURL, bytes.NewBuffer(data))
	if err != nil {
		logf("error creating request for %d", characterID)
		return ""
	}

	req.Header.Set("User-Agent", "karkinos.ga, POC: Yonneh in #esi")
	req.Header.Set("Authorization", "Basic "+encodeAuthToken(c.Oauth["CCP"].ClientID, c.Oauth["CCP"].ClientSecret))
	req.Header.Set("Content-type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		logf("error sending request for %d", characterID)
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		logf("error %s from CCP for %d", resp.Status, characterID)
		return ""
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logf("error reading request for %d", characterID)
		return ""
	}
	var responseData tokenResponse
	if err := json.Unmarshal(body, &responseData); err != nil {
		logf("error Unmarshaling token for %d", characterID)
		return ""
	}
	tokens[characterID].AccessToken = responseData.AccessToken
	tokens[characterID].Expires = (ktime() + (responseData.ExpiresIn * 1000)) - 10000
	stmt := safePrepare(fmt.Sprintf("UPDATE `%s`.`%s` SET access_token=?,access_token_exp=? where id=?", c.Tables["chars"].DB, c.Tables["chars"].Name))
	defer stmt.Close()
	res, err := stmt.Exec(tokens[characterID].AccessToken, tokens[characterID].Expires, characterID)
	if err != nil {
		logf("error updating SQL for %d", characterID)
		return ""
	}
	aff, _ := res.RowsAffected()
	if aff != 1 {
		logf("error updating SQL for %d, no rows affected", characterID)
		return ""
	}
	// log( fmt.Sprintf("New token returned for %d", characterID))
	return tokens[characterID].AccessToken
}

func verifyAccessToken(characterID uint64) {
	token := getAccessToken(characterID, "")
	if len(token) < 5 {
		log("no token available")
		return
	}
	req, err := http.NewRequest("GET", c.Oauth["CCP"].VerifyURL, nil)
	if err != nil {
		logf("error creating request for %d", characterID)
		return
	}
	req.Header.Set("User-Agent", "karkinos.ga, POC: Yonneh in #esi")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	if err != nil {
		logf("error sending request for %d", characterID)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		logf("error %s from CCP for %d", resp.Status, characterID)
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logf("error reading request for %d", characterID)
		return
	}
	var responseData verifyResponse
	if err := json.Unmarshal(body, &responseData); err != nil {
		logf("error Unmarshaling verification for %d", characterID)
		return
	}
	tokens[characterID].Scopes = strings.Split(responseData.Scopes, " ")
	stmt := safePrepare(fmt.Sprintf("UPDATE `%s`.`%s` SET name=?,scopes=? where id=?", c.Tables["chars"].DB, c.Tables["chars"].Name))
	defer stmt.Close()
	_, err = stmt.Exec(responseData.CharacterName, responseData.Scopes, characterID)
	if err != nil {
		logf("error updating SQL for %d", characterID)
		return
	}
	return
}
