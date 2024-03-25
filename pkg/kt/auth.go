package kt

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/IBM/sarama"
)

type AuthConfig struct {
	Mode          string `json:"mode"`
	CACert        string `json:"ca"`
	ClientCert    string `json:"cert"`
	ClientCertKey string `json:"key"`
	SASLUsr       string `json:"usr"`
	SASLPwd       string `json:"pwd"`
	SASLVersion   *int   `json:"ver"`
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

	data := []byte(fn)

	if _, err := os.Stat(fn); err == nil {
		data, err = os.ReadFile(fn)
		if err != nil {
			return fmt.Errorf("failed to read auth file, error %q", err)
		}
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
	sc.Net.SASL.Handshake = true
	sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	version, err := SASLVersion(sc.Version, t.SASLVersion)
	if err != nil {
		return err
	}
	sc.Net.SASL.Version = version
	return nil
}

func SASLVersion(kafkaVersion sarama.KafkaVersion, saslVersion *int) (int16, error) {
	if saslVersion == nil {
		if kafkaVersion.IsAtLeast(sarama.V1_0_0_0) {
			return sarama.SASLHandshakeV1, nil
		}
		return sarama.SASLHandshakeV0, nil
	}

	switch *saslVersion {
	case 0:
		return sarama.SASLHandshakeV0, nil
	case 1:
		return sarama.SASLHandshakeV1, nil
	default:
		return 0, errors.New("invalid SASL version")
	}
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

	caString, err := os.ReadFile(caCert)
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
