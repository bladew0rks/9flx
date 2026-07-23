package p9fs

import (
	"crypto/subtle"
	"errors"
	"io"

	"github.com/emersion/go-sasl"
)

func PasswordAuth(username, password string) func(io.ReadWriter) (string, error) {
	return func(stream io.ReadWriter) (string, error) {
		authenticated := ""
		server := sasl.NewPlainServer(func(identity, suppliedUser, suppliedPassword string) error {
			userOK := subtle.ConstantTimeCompare([]byte(suppliedUser), []byte(username))
			passwordOK := subtle.ConstantTimeCompare([]byte(suppliedPassword), []byte(password))
			identityOK := identity == "" || subtle.ConstantTimeCompare([]byte(identity), []byte(username)) == 1
			if userOK != 1 || passwordOK != 1 || !identityOK {
				return errors.New("invalid credentials")
			}
			authenticated = username
			return nil
		})
		buffer := make([]byte, 4096)
		for {
			n, err := stream.Read(buffer)
			if err != nil {
				return "", err
			}
			challenge, done, err := server.Next(buffer[:n])
			if err != nil {
				return "", errors.New("authentication failed")
			}
			if len(challenge) > 0 {
				if _, err := stream.Write(challenge); err != nil {
					return "", err
				}
			}
			if done {
				return authenticated, nil
			}
		}
	}
}
