// ESI Token Management

package main

type token struct {
	characterID  uint64
	corpID       uint64
	refreshToken string
	accessToken  string
	expires      int64
}
