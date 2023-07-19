package kt

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/IBM/sarama"
)

type AuthConfig struct {
	Mode          string `json:"mode"`
	CACert        string `json:"ca-cert"`
	ClientCert    string `json:"client-cert"`
	ClientCertKey string `json:"client-cert-key"`
	SASLUsr       string `json:"sasl-usr"`
	SASLPwd       string `json:"sasl-pwd"`
}

func (t *AuthConfig) ReadConfigFile(fileName string) error {
	envFileName := os.Getenv(EnvAuth)
	if fileName == "" && envFileName == "" {
		return nil
	}

	fn := fileName
	if fn == "" {
		fn = envFileName
	}

	data, err := os.ReadFile(fn)
	if err != nil {
		return fmt.Errorf("failed to read auth file, error %q", err)
	}

	if err := json.Unmarshal(data, t); err != nil {
		return fmt.Errorf("failed to unmarshal auth file, error %q", err)
	}

	return nil
}

func (t AuthConfig) SetupAuth(sc *sarama.Config) error {
	switch t.Mode {
	case "":
		return nil
	case "TLS":
		return t.setupAuthTLS(sc)
	case "TLS-1way":
		return t.setupAuthTLS1Way(sc)
	case "SASL":
		return t.setupSASL(sc)
	default:
		return fmt.Errorf("unsupport auth mode: %#v", t.Mode)
	}
}

func (t AuthConfig) setupSASL(sc *sarama.Config) error {
	sc.Net.SASL.Enable = true
	sc.Net.SASL.User = t.SASLUsr
	sc.Net.SASL.Password = t.SASLPwd
	return nil
}

func (t AuthConfig) setupAuthTLS1Way(sc *sarama.Config) error {
	sc.Net.TLS.Enable = true
	sc.Net.TLS.Config = &tls.Config{}
	return nil
}

func (t AuthConfig) setupAuthTLS(sc *sarama.Config) error {
	tlsCfg, err := createTLSConfig(t.CACert, t.ClientCert, t.ClientCertKey)
	if err != nil {
		return err
	}

	sc.Net.TLS.Enable = true
	sc.Net.TLS.Config = tlsCfg

	return nil
}

func createTLSConfig(caCert, clientCert, certKey string) (*tls.Config, error) {
	if caCert == "" || clientCert == "" || certKey == "" {
		return nil, fmt.Errorf("a-cert, client-cert and client-key are required")
	}

	caString, err := ioutil.ReadFile(caCert)
	if err != nil {
		return nil, fmt.Errorf("failed to read ca-cert err=%v", err)
	}

	caPool := x509.NewCertPool()
	if ok := caPool.AppendCertsFromPEM(caString); !ok {
		return nil, fmt.Errorf("unable to add ca-cert at %s to certificate pool", caCert)
	}

	cert, err := tls.LoadX509KeyPair(clientCert, certKey)
	if err != nil {
		return nil, err
	}

	tlsCfg := &tls.Config{RootCAs: caPool, Certificates: []tls.Certificate{cert}}
	return tlsCfg, nil
}
